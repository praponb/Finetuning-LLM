from pyspark.sql import functions as F

result = (
    df_orders.alias("o")
    .join(df_shipments.filter(F.month("delivered_date").isin([1, 2, 3])).alias("s"), F.col("o.id") == F.col("s.order_id"), "left_semi")
    .filter(F.col("o.channel") == "mobile")
    .select(
        F.col("o.id"),
        F.col("o.customer_id"),
        F.col("o.store_id"),
        F.col("o.amount"),
    )
    .orderBy(F.col("id").desc())
)
