from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2024-01-01", "2024-09-01"))
    .select(F.col("shipped_date").alias("event_date"))
    .intersect(df_accounts.select(F.col("opened_date").alias("event_date")))
)
