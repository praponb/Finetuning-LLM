from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.filter(F.col("order_date").between("2024-02-18", "2024-09-04")).alias("o"), F.col("s.order_id") == F.col("o.id"), "left_semi")
    .filter(F.col("s.carrier").isNotNull())
    .select(F.col("s.id"), F.col("s.delivered_date"))
    .orderBy(F.col("delivered_date").desc(), "id")
    .limit(50)
)
