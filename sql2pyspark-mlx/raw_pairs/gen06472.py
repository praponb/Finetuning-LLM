from pyspark.sql import functions as F

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("adjusted_unit_price", F.round((F.col("unit_price") * 1.15), 2))
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
)

result = (
    cleaned
    .alias("a")
    .join(df_orders.alias("p"), F.col("a.order_id") == F.col("p.id"), "left")
    .rollup(F.col("p.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.max(F.col("a.quantity")).alias("max_quantity"),
        F.grouping_id().alias("grouping_level"),
    )
)
