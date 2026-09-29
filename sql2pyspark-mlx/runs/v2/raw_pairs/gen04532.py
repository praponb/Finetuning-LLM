from pyspark.sql import functions as F

result = (
    df_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(F.countDistinct("amount").alias("unique_orders"))
    .orderBy(F.col("unique_orders").desc())
)
