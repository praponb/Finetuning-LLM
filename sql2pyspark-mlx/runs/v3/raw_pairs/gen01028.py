from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status") == "REFUNDED")
    .select("status")
    .intersect(df_subscriptions.select("status"))
)
