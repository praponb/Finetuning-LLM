from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("plan").orderBy("signup_date_dayofweek", "id")
w_latest_2 = Window.partitionBy("plan").orderBy(F.col("birth_year").desc(), F.col("id").desc())

recent_listeners = (
    df_listeners
    .select(
        "id",
        "plan",
        "birth_year",
        "interests",
        F.dayofweek(F.col("signup_date")).alias("signup_date_dayofweek"),
    )
)

unique_rows = (
    recent_listeners
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

unique_rows_2 = (
    unique_rows
    .withColumn("recency_rank", F.row_number().over(w_latest_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

plan_stats = (
    unique_rows_2
    .groupBy("plan")
    .agg(F.max("birth_year").alias("max_birth_year"))
)

compared = (
    unique_rows_2.alias("s")
    .join(plan_stats.alias("g"), F.col("g.plan") == F.col("s.plan"), "inner")
    .filter(F.col("s.birth_year") == F.col("g.max_birth_year"))
    .select(
        "s.*",
        "g.max_birth_year",
        F.round(F.col("s.birth_year") - F.col("g.max_birth_year"), 2).alias("birth_year_minus_max"),
    )
)

result = (
    compared
    .rollup("plan", "signup_date_dayofweek")
    .agg(
        F.round(F.sum("birth_year_minus_max"), 2).alias("total_birth_year_minus_max"),
        F.round(F.avg("birth_year"), 2).alias("avg_birth_year"),
        F.min("birth_year_minus_max").alias("min_birth_year_minus_max"),
    )
)
