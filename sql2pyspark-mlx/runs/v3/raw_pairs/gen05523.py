from pyspark.sql import functions as F

base = df_inventory.filter(F.col("quantity").between(74, 191))

summary = (
    base
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = summary.filter(F.col("max_quantity") < 60)
