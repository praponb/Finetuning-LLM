from pyspark.sql import functions as F

src = df_order_items.filter(F.col("quantity") != 4)

agg = (
    src
    .groupBy("product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
)

result = (
    agg
    .select("product_id", "row_count", "avg_quantity")
    .orderBy("row_count")
)
