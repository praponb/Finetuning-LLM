from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([1, 19, 30]) | F.array_contains("tags", "bestseller"))
    .groupBy("brand")
    .agg(
        (F.max("cost") - F.min("cost")).alias("cost_range"),
        F.round(F.stddev("price"), 2).alias("stddev_price"),
        F.avg("cost").alias("avg_cost"),
    )
)
