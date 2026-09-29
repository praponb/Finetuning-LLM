from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .filter(F.col("s.order_id").isNotNull())
    .join(df_orders.filter(F.col("amount").isNotNull()).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_anti")
    .select(
        F.col("s.id"),
        F.col("s.shipped_date"),
        F.col("s.carrier"),
        F.col("s.status"),
    )
    .orderBy("carrier")
)
