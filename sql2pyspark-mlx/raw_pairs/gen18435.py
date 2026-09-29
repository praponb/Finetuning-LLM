from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("amount").between(830, 1570)).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(~F.col("s.status").isin(["RETURNED", "DELIVERED", "IN_TRANSIT"]))
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.order_id"),
        F.col("s.carrier"),
    )
    .orderBy("carrier")
)
