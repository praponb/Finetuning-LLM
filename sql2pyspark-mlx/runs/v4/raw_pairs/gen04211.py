from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-12-01", "2025-07-22") & (F.col("shipped_date") > "2023-04-01"))
    .withColumn("delivered_date_label", F.date_format("delivered_date", "yyyy-MM-dd"))
    .orderBy(F.col("delivered_date").desc(), "id")
    .limit(5)
)
