from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("signup_date"), "country").orderBy("signup_date", "id")
w2 = Window.partitionBy("country").orderBy("age_rounded", "id")
w3 = Window.partitionBy("segment").orderBy("age_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_customers
    .filter(F.col("segment").isNotNull())
    .select(
        "id",
        "name",
        "country",
        "city",
        "segment",
        "signup_date",
        "credit_limit",
        "age",
        F.when(F.col("is_active"), "Yes").otherwise("No").alias("is_active_label"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

filtered = (
    base
    .filter(F.col("city") == "Bangkok")
    .select(
        "id",
        "country",
        "segment",
        "signup_date",
        "is_active_label",
        "city_clean",
        F.round("age", 2).alias("age_rounded"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_filled"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.lead("age_rounded", 1).over(w).alias("next_age_rounded"),
        F.lag("age_rounded", 1).over(w2).alias("prev_age_rounded"),
    )
)

enriched_customers = (
    enriched
    .select(
        "*",
        F.sum("prev_age_rounded").over(w3).alias("running_sum_prev_age_rounded"),
        F.count("prev_age_rounded").over(w3).alias("running_count_prev_age_rounded"),
        F.max("prev_age_rounded").over(w3).alias("running_max_prev_age_rounded"),
    )
)

filtered_customers = (
    enriched_customers
    .filter(F.col("running_sum_prev_age_rounded") > 190)
    .withColumn("signup_date_label", F.date_format("signup_date", "yyyy-MM"))
)

filtered_customers_2 = (
    filtered_customers
    .filter((F.col("signup_date") >= F.lit("2023-09-12").cast("date")) & F.col("segment").isNotNull())
    .select(
        "*",
        F.when(F.col("segment") == "Consumer", 1).otherwise(0).alias("is_consumer"),
        F.coalesce("prev_age_rounded", F.lit(0)).alias("prev_age_rounded_filled"),
        F.coalesce("segment", F.lit("DIRECT")).alias("segment_clean"),
    )
)

result = (
    filtered_customers_2
    .groupBy(
        F.date_trunc("year", "signup_date").alias("year_start"),
        "country",
        F.date_trunc("month", "signup_date").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("age_rounded", [0.25, 0.5, 0.75]).alias("age_rounded_quartiles"),
        F.round(F.avg("prev_age_rounded_filled"), 2).alias("avg_prev_age_rounded_filled"),
    )
    .orderBy(F.col("year_start").desc())
)
