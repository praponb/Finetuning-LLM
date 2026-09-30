from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("country").orderBy("days_since_signup_date", "id")
w_country_signup_date_year_by = Window.partitionBy("country", F.year("signup_date")).orderBy(F.col("birth_year_distance").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_plan = Window.partitionBy("plan")
w_dedup = Window.partitionBy("country").orderBy(F.col("second_days_since_signup_date").desc(), F.col("id").desc())

recent = (
    df_listeners
    .filter((F.col("plan") != "free") & (F.array_contains(F.col("interests"), "indie")))
    .select(
        "id",
        "plan",
        "country",
        "signup_date",
        "interests",
        F.concat_ws("-", F.col("country"), F.col("plan")).alias("country_plan_label"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("signup_date")).alias("days_since_signup_date"),
        F.abs(F.col("birth_year") - 1980).alias("birth_year_distance"),
    )
)

latest_per_key = (
    recent
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

country_stats = (
    latest_per_key
    .groupBy("country")
    .agg(F.max("birth_year_distance").alias("max_birth_year_distance"))
)

compared = (
    latest_per_key.alias("s")
    .join(country_stats.alias("g"), F.col("g.country") == F.col("s.country"), "inner")
    .filter(F.col("s.birth_year_distance") == F.col("g.max_birth_year_distance"))
    .select(
        "s.*",
        "g.max_birth_year_distance",
        F.round(F.col("s.birth_year_distance") - F.col("g.max_birth_year_distance"), 2).alias("birth_year_distance_minus_max"),
    )
)

scored = (
    compared
    .select(
        "*",
        F.length(F.trim(F.col("country_plan_label"))).alias("country_plan_label_len"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.nth_value("days_since_signup_date", 2).over(w_country_signup_date_year_by).alias("second_days_since_signup_date"),
        F.size(F.collect_set("country").over(w_plan)).alias("distinct_country_in_plan"),
    )
)

latest_per_key_2 = (
    with_windows
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key_2
    .groupBy("country_plan_label_len")
    .pivot("plan", ["free", "family", "student", "premium"])
    .agg(F.sum("birth_year_distance"))
)
