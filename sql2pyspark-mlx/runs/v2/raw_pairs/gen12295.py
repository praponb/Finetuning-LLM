from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("status") == "COMPLETED").alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(~F.col("s.status").isin(["RETURNED", "IN_TRANSIT"]))
    .select(F.col("s.id"), F.col("s.status"), F.col("s.carrier"))
)
