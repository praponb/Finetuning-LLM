from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("id", "customer_id", F.initcap(F.trim("status")).alias("status_title"))
)
