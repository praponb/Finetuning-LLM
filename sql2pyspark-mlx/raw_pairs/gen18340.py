from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "cost", "shipped_date", "order_id")
    .orderBy("id", F.col("order_id").desc())
)
