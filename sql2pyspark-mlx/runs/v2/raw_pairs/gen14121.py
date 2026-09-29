from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-03-01", "2024-06-17") & (F.col("shipped_date") <= "2024-09-22"))
    .select("id", "cost", "status", "delivered_date")
    .orderBy("status")
)
