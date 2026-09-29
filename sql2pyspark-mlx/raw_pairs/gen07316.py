from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country")
w2 = Window.partitionBy("segment")
w3 = Window.orderBy("age", "id")
w4 = Window.partitionBy("city", "segment").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy(F.year("signup_date"), "city").orderBy("signup_date", "id").rowsBetween(-5, Window.currentRow)
w_hist = Window.partitionBy("country").orderBy("age_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_customers
    .filter((F.col("signup_date") >= F.lit("2023-07-28").cast("date")) & F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "email",
        "country",
        "city",
        "segment",
        "signup_date",
        "is_active",
        "age",
        F.lower("country").alias("country_lower"),
    )
)

cleaned = (
    filtered
    .filter(F.col("segment").isNotNull())
    .select(
        "*",
        F.upper("country").alias("country_upper"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

metrics = (
    cleaned
    .select(
        "id",
        "country",
        "segment",
        "is_active",
        "age",
        "country_lower",
        "country_upper",
        "city_filled",
        F.round((F.col("age") / F.sum("age").over(w)), 4).alias("age_share"),
        F.round((F.col("age") / F.sum("age").over(w2)), 4).alias("age_share_2"),
        F.lead("age", 1).over(w3).alias("next_age"),
        F.sum("age").over(w4).alias("cumulative_age"),
        F.avg("age").over(w5).alias("age_avg_5"),
    )
)

metrics_customers = (
    metrics
    .filter(F.col("age_share").isNotNull())
    .select(
        "id",
        "country_lower",
        "country_upper",
        "city_filled",
        "age_share",
        "next_age",
        F.sum("next_age").over(w_hist).alias("running_sum_next_age"),
        F.count("next_age").over(w_hist).alias("running_count_next_age"),
        F.avg("next_age").over(w_hist).alias("running_avg_next_age"),
    )
)

result = (
    metrics_customers
    .groupBy(
        F.when(F.col("next_age") >= 68, "PREMIUM").otherwise("STANDARD").alias("next_age_tier"),
        F.when(F.col("running_avg_next_age") >= 58, "PREMIUM").otherwise("STANDARD").alias("running_avg_next_age_tier"),
    )
    .agg(
        F.round(F.avg("running_avg_next_age"), 2).alias("avg_running_avg_next_age"),
        F.round(F.avg("running_avg_next_age"), 2).alias("avg_running_avg_next_age_2"),
        F.round(F.avg("running_avg_next_age"), 2).alias("avg_running_avg_next_age_3"),
        F.min("age_share").alias("min_age_share"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("min_age_share") > 0)
    .orderBy(F.col("next_age_tier").asc_nulls_first())
)
