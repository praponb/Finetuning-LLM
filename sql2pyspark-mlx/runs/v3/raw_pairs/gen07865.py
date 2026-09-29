from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("plan") != "standard")
    .orderBy("customer_id")
)
