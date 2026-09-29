from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select("start_date")
    .union(df_shipments.select("delivered_date"))
)
