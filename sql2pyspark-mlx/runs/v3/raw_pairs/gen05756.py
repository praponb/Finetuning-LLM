from pyspark.sql import functions as F

min_shipped_date = df_shipments.filter(F.col("shipped_date") >= "2024-01-01").agg(F.min("shipped_date")).first()[0]

result = (
    df_shipments
    .filter(F.col("shipped_date") == min_shipped_date)
    .select("id", "shipped_date", "status", "delivered_date")
)
