from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("order_date").between("2025-09-01", "2025-10-01")).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.carrier").isin(["UPS", "Kerry"]))
    .select(F.col("s.id"), F.col("s.shipped_date"))
    .orderBy("shipped_date", F.col("id").desc())
)
