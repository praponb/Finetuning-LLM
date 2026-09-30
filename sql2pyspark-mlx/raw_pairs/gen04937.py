from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("status")
    .intersect(df_transactions.select("merchant"))
)
