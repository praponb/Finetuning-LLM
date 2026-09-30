from pyspark.sql import functions as F

src = df_order_items.filter(F.col("product_id").isNull() & (F.col("unit_price") > 250))

summary = (
    src
    .groupBy("product_id")
    .agg(
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
)

result = summary.select("product_id", "avg_quantity", "revenue")
