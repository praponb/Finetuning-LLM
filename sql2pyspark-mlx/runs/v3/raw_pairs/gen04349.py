from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "inner")
    .filter((F.col("o.customer_id") == 11) & (F.col("s.carrier") == "DHL"))
    .select(
        F.col("s.id"),
        F.col("s.carrier"),
        F.col("s.shipped_date"),
        F.col("o.channel"),
        F.datediff(F.current_date(), F.col("s.shipped_date")).alias("days_since_shipped_date"),
    )
    .orderBy("shipped_date", F.col("carrier").desc())
)
