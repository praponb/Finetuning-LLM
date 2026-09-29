from pyspark.sql import functions as F

src = df_order_items.filter(F.col("quantity").between(4, 6))

product_id_summary = (
    src
    .groupBy("product_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.max("quantity").alias("max_quantity"),
    )
)

result = (
    product_id_summary
    .filter(F.col("stddev_unit_price") >= 45)
    .select("product_id", "stddev_unit_price", "max_quantity")
)
