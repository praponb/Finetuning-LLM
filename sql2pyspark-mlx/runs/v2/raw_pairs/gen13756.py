from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 28)
    .select("status")
    .subtract(df_subscriptions.select("status"))
)
