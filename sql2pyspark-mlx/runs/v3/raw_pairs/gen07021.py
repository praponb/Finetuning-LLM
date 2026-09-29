from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .groupBy("order_id")
    .agg(
        F.round(F.stddev("unit_price"), 2).alias("stddev_unit_price"),
        F.sum((F.col("quantity") * F.col("unit_price"))).alias("revenue"),
    )
    .orderBy(F.col("revenue").desc(), "order_id")
    .limit(5)
)
