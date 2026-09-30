from pyspark.sql import functions as F

result = (
    df_payments
    .filter(((F.col("status") != "REFUNDED") & (F.col("method") != "cash")) | F.col("order_id").isin([12, 16]))
    .select("status", F.hour("paid_at").alias("paid_at_hour"))
    .orderBy("status")
)
