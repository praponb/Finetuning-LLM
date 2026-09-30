from pyspark.sql import functions as F

valid = (
    df_inventory
    .filter(F.col("quantity") > 76)
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 1))
)

result = (
    valid
    .groupBy(
        F.date_trunc("quarter", "last_restock_plus_months").alias("quarter_start"),
        F.date_trunc("month", "last_restock_plus_months").alias("activity_month"),
    )
    .agg(
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .orderBy(F.col("quantity_p95").desc_nulls_last())
)
