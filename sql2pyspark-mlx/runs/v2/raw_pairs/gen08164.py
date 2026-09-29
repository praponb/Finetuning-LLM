from pyspark.sql import functions as F

src = df_order_items.filter(F.col("order_id").isin([4, 13, 20, 23]))

agg = (
    src
    .groupBy("product_id")
    .agg(
        F.sum("quantity").alias("total_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = agg.select("product_id", "total_quantity", "revenue")
