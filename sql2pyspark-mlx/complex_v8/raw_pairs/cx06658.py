from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_plan_country_by_signup_date = Window.partitionBy("plan", "country").orderBy(F.col("signup_date").desc())
w_plan_country_by_signup_date_2 = Window.partitionBy("plan", "country").orderBy(F.col("signup_date").desc(), "id")
w_signup_date_month_country_by = Window.partitionBy(F.month("signup_date"), "country").orderBy(F.col("signup_date").asc_nulls_last())
w_country_by_signup_date_full = Window.partitionBy("country").orderBy("signup_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_plan_country_by_signup_date_3 = Window.partitionBy("plan", "country").orderBy(F.col("signup_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_dedup = Window.partitionBy("country").orderBy("second_signup_date_2", "id")

prepared = (
    df_listeners
    .filter((F.size(F.col("interests")) >= 2) & (F.col("plan").isNotNull()))
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "interests",
    )
)

windowed = (
    prepared
    .select(
        "id",
        "username",
        "plan",
        "country",
        "signup_date",
        "interests",
        F.round(F.percent_rank().over(w_plan_country_by_signup_date), 3).alias("signup_date_percent_rank"),
        F.lead("signup_date").over(w_plan_country_by_signup_date_2).alias("next_signup_date"),
        F.dense_rank().over(w_signup_date_month_country_by).alias("signup_date_dense_rank"),
        F.nth_value("signup_date", 2).over(w_country_by_signup_date_full).alias("second_signup_date"),
        F.nth_value("signup_date", 2).over(w_plan_country_by_signup_date_3).alias("second_signup_date_2"),
    )
)

enriched = (
    windowed
    .filter(F.upper(F.trim(F.col("username"))).like("G%"))
    .select(
        "*",
        F.abs(F.col("signup_date_dense_rank") - 6).alias("signup_date_dense_rank_distance"),
        F.concat(F.col("username"), F.lit("#"), F.col("id").cast("string")).alias("username_ref"),
        F.date_trunc("quarter", F.col("signup_date")).alias("signup_date_quarter"),
    )
)

latest_per_key = (
    enriched
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select("id", "signup_date_dense_rank_distance", "signup_date_dense_rank")
    .unpivot(
        ids=["id"],
        values=["signup_date_dense_rank_distance", "signup_date_dense_rank"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
