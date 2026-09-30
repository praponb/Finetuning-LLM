from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("customer_id") == 19).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.shipped_date").between("2023-05-01", "2023-10-01"))
    .select(F.col("s.id"), F.col("s.status"), F.col("s.carrier"))
    .orderBy("status")
)
