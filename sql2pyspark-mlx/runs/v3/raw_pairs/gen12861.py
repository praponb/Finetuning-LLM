from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(~F.col("status").isin(["COMPLETED", "RETURNED", "SHIPPED"])).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .select(F.col("s.id"), F.col("s.carrier"))
)
