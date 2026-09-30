from pyspark.sql import functions as F

result = (
    df_inventory
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 206, "HIGH").otherwise("LOW"))
    .withColumn("year_start", F.date_trunc("YEAR", "last_restock"))
    .groupingSets(
        [["product_id", "quantity_tier", "year_start"], ["product_id", "quantity_tier"]],
        "product_id",
        "quantity_tier",
        "year_start",
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.stddev_samp("quantity").alias("quantity_stddev_2"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("quantity_p90") > 0)
    .orderBy(F.col("quantity_stddev").asc_nulls_last())
)
