from pyspark.sql import functions as F

valid = (
    df_policies
    .filter((~F.col("product").isin("LIFE", "HOME")) & (F.col("start_date") < F.lit("2025-02-13").cast("date")))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.lower(F.regexp_replace(F.trim(F.col("product")), " ", "_")).alias("product_slug"),
    )
)

status_stats = (
    valid
    .groupBy("status")
    .agg(F.percentile_approx("premium", 0.5).alias("median_premium"))
)

above_average = (
    valid.alias("s")
    .join(status_stats.alias("g"), F.col("g.status") == F.col("s.status"), "inner")
    .filter(F.col("s.premium") >= F.col("g.median_premium"))
    .select(
        "s.*",
        "g.median_premium",
        F.round(F.col("s.premium") - F.col("g.median_premium"), 2).alias("premium_minus_median"),
    )
)

result = (
    above_average
    .groupBy("end_date")
    .pivot("status", ["ACTIVE", "CANCELLED"])
    .agg(F.sum("median_premium").alias("total"), F.count("median_premium").alias("n"))
)
