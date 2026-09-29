from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "cost", "delivered_date")
    .orderBy("cost")
)
