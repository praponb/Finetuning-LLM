from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNotNull())
    .select("id", "delivered_date", "order_id")
)
