from pyspark.sql import functions as F

max_shipped_date = df_shipments.agg(F.max("shipped_date")).first()[0]

result = (
    df_shipments
    .filter(F.col("shipped_date") > max_shipped_date)
    .select("id", "shipped_date", "delivered_date", "status")
)
