from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost") < 690)
    .select("price")
    .unionByName(df_subscriptions.select(F.col("monthly_fee").alias("price")))
)
