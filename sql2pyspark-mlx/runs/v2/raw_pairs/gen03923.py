from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.alias("o"), F.col("oi.order_id") == F.col("o.id"), "inner")
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("o.store_id"),
        F.when(F.col("o.amount") >= 1210, "High").when(F.col("o.amount") >= 220, "Medium").otherwise("Low").alias("amount_band"),
    )
    .orderBy(F.col("id").desc())
)
