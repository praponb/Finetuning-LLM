from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-11-01", "2024-12-01"))
    .select("id", "shipped_date", "cost")
    .orderBy("cost", F.col("id").desc())
    .limit(10)
)
