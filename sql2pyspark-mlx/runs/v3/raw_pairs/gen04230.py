from pyspark.sql import functions as F

src = (
    df_order_items
    .filter((F.col("order_id") == 4) & (F.col("unit_price").between(140, 280) | (F.col("unit_price") >= 190)))
)

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.sum((F.col("unit_price") * F.col("quantity"))).alias("revenue"),
    )
)

result = product_id_summary.filter(F.col("max_quantity") < 7.1)
