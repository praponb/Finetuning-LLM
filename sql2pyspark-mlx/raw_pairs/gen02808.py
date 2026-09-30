from pyspark.sql import functions as F

base = (
    df_order_items
    .filter(F.col("product_id").isNull() & ((F.col("quantity") > 7) | (F.col("quantity") == 5)))
)

product_id_summary = (
    base
    .groupBy("product_id")
    .agg(F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"))
)

result = (
    product_id_summary
    .filter(F.col("revenue") > 52)
    .select("product_id", "revenue")
    .orderBy("revenue")
)
