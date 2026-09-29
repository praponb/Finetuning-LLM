from pyspark.sql import functions as F

result = (
    df_subscriptions
    .select("start_date", F.lit("subscription").alias("source"))
    .union(df_products.select("launch_date", F.lit("product").alias("source")))
    .orderBy("source")
)
