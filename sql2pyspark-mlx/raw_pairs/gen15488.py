from pyspark.sql import functions as F

min_delivered_date = df_shipments.agg(F.min("delivered_date")).first()[0]

result = (
    df_shipments
    .filter(F.col("delivered_date") <= min_delivered_date)
    .select("id", "delivered_date", "cost", "shipped_date")
    .orderBy("shipped_date", "id")
    .limit(5)
)
