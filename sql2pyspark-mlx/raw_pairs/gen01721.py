from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-07-01", "2024-02-01"))
    .select("status", "order_id")
    .intersect(df_shipments.filter(F.col("cost") <= 26).select("status", "order_id"))
)
