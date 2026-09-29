from pyspark.sql import functions as F

recent_order_items = (
    df_order_items
    .filter(F.col("quantity").isNotNull() & F.col("order_id").isin([6, 14]))
)

agg = (
    recent_order_items
    .groupBy("order_id")
    .agg(
        F.sum("unit_price").alias("total_unit_price"),
        F.max("unit_price").alias("max_unit_price"),
    )
)

result = (
    agg
    .filter(F.col("max_unit_price") > 160)
    .select("order_id", "total_unit_price", "max_unit_price")
)
