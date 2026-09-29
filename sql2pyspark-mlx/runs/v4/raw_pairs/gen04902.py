from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id", F.date_trunc("QUARTER", "last_restock").alias("quarter_start"))
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
)
