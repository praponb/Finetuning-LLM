from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .filter(F.col("s.order_id").isNotNull())
    .join(df_orders.filter(F.col("shipping_fee") <= 14).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .filter(F.col("s.shipped_date") > "2024-05-01")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.cost"),
        F.col("s.order_id"),
    )
)
