from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([4, 6, 11]))
    .select("start_date")
    .subtract(df_products.select(F.col("launch_date").alias("start_date")))
    .orderBy("start_date")
)
