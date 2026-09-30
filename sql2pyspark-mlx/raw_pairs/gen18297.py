from pyspark.sql import functions as F

result = (
    df_order_items
    .groupBy("product_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
    .filter(F.col("max_unit_price") >= 270)
    .select("product_id", "max_unit_price", "revenue")
    .orderBy("max_unit_price", F.col("product_id").desc())
)
