from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-01-01", "2024-02-01"))
    .select("id", "shipped_date", "delivered_date")
    .orderBy("shipped_date", "id")
    .limit(5)
)
