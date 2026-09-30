from pyspark.sql import functions as F

src = df_inventory.filter(F.col("product_id").isin([1, 5, 10]))

agg = (
    src
    .groupBy("product_id")
    .agg(F.round(F.avg("quantity"), 2).alias("avg_quantity"))
)

result = (
    agg
    .select("product_id", "avg_quantity")
    .orderBy("product_id")
)
