from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.col("delivered_date").between("2024-06-01", "2024-10-21")).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .filter(F.col("o.store_id").isin([4, 13]))
    .select(
        F.col("o.id"),
        F.col("o.store_id"),
        F.col("o.amount"),
        F.col("o.customer_id"),
    )
)
