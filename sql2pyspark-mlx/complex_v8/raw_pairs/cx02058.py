from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("country").orderBy("birth_year_distance", "id")
w_plan_country_by_birth_year_p = Window.partitionBy("plan", "country").orderBy(F.col("birth_year").asc_nulls_last(), "id").rowsBetween(-2, -1)
w_plan_country_by_birth_year_t = Window.partitionBy("plan", "country").orderBy(F.col("birth_year").asc_nulls_last())
w_plan = Window.partitionBy("plan")
w_plan_by_birth_year_ties = Window.partitionBy("plan").orderBy("birth_year")

valid_listeners = (
    df_listeners
    .filter(F.array_contains(F.col("interests"), "podcasts"))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "birth_year",
        "interests",
        F.date_format(F.col("signup_date"), "yyyy-MM-dd").alias("signup_date_label"),
        F.abs(F.col("birth_year") - 1970).alias("birth_year_distance"),
    )
)

unique_rows = (
    valid_listeners
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

sequenced = (
    unique_rows
    .select(
        "id",
        "plan",
        "country",
        "birth_year",
        "interests",
        "signup_date_label",
        "birth_year_distance",
        F.round(F.avg("birth_year").over(w_plan_country_by_birth_year_p), 2).alias("birth_year_avg_2_prior"),
        F.round(F.cume_dist().over(w_plan_country_by_birth_year_t), 3).alias("birth_year_cume_dist"),
        F.max("birth_year_distance").over(w_plan).alias("plan_max_birth_year_distance"),
        F.size(F.collect_set("country").over(w_plan)).alias("distinct_country_in_plan"),
        F.rank().over(w_plan_by_birth_year_ties).alias("birth_year_rank"),
    )
)

filtered = (
    sequenced
    .crossJoin(sequenced.agg(F.max("birth_year_rank").alias("overall_max_birth_year_rank")))
    .filter(F.col("birth_year_rank") < F.col("overall_max_birth_year_rank"))
    .drop("overall_max_birth_year_rank")
)

result = (
    filtered
    .select("id", "birth_year_avg_2_prior", "birth_year_cume_dist")
    .unpivot(
        ids=["id"],
        values=["birth_year_avg_2_prior", "birth_year_cume_dist"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
