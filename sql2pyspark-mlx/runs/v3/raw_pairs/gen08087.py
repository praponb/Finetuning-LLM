from pyspark.sql import functions as F

result = (
    df_payments
    .filter((F.col("amount") >= 480) & (F.col("order_id") == 3) & F.col("order_id").isNotNull())
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(F.countDistinct("amount").alias("unique_orders"))
    .filter(F.col("unique_orders") > 3)
)
