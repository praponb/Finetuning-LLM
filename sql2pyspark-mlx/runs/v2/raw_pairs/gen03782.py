from pyspark.sql import functions as F

result = (
    df_customers.alias("c")
    .join(df_subscriptions.filter(F.col("plan") == "standard").alias("s"), F.col("c.id") == F.col("s.customer_id"), "left_anti")
    .filter(F.col("c.signup_date") < "2024-02-01")
    .select(F.col("c.id"), F.col("c.city"))
    .orderBy("city")
)
