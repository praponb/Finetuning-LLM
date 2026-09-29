from pyspark.sql import functions as F

cleaned = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (F.col("brand").eqNullSafe(F.lit("Umbrella"))))
    .withColumn("price_filled", F.coalesce("price", F.lit(0)))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("UNKNOWN")))
)

result = (
    cleaned
    .withColumn("quarter_start", F.date_trunc("QUARTER", "launch_date"))
    .groupingSets(
        [["category", "quarter_start"], ["category"]],
        "category",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("price", 0.9).alias("price_p90"),
        F.count("*").alias("row_count"),
    )
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
