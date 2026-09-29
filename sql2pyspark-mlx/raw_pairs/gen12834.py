from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost", "delivered_date", "order_id")
    .orderBy(F.col("order_id").desc())
)
