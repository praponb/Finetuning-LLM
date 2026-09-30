from pyspark.sql import functions as F

result = (
    df_customers
    .select("email", F.lit("customer").alias("kind"))
    .union(df_orders.select("channel", F.lit("order").alias("kind")))
    .orderBy("email")
)
