from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity").desc_nulls_last(), "id")

ranked = (
    df_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .withColumn("quantity_tier", F.when(F.col("quantity") >= 246, "HIGH").otherwise("LOW"))
    .rollup("quantity_tier", "product_id")
    .agg(
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.max("quantity").alias("max_quantity"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.count("*").alias("row_count"),
        F.grouping_id().alias("grouping_id"),
    )
)
