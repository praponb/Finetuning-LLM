from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
    )
    .filter(F.col("avg_quantity") >= 0)
    .orderBy(F.col("quantity_p90").asc_nulls_last())
)
