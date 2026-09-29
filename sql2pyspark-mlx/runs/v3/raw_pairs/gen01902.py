from pyspark.sql import functions as F

result = (
    df_order_items.alias("oi")
    .join(df_orders.filter(F.col("status").isin(["RETURNED", "COMPLETED"])).alias("o"), F.col("oi.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("oi.order_id").isin([18, 19, 32]))
    .select(
        F.col("oi.id"),
        F.col("oi.unit_price"),
        F.col("oi.quantity"),
        F.col("oi.order_id"),
    )
    .orderBy(F.col("order_id").desc())
)
