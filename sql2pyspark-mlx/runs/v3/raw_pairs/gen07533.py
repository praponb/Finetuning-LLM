from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.alias("s"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter(F.col("o.amount") > 1220)
    .select(
        F.col("o.customer_id"),
        F.col("o.amount"),
        F.col("o.discount"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("s.cost"),
    )
)
