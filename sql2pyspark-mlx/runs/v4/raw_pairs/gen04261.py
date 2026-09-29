from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date", F.lit("order").alias("source"))
    .union(df_stores.select("opened_date", F.lit("store").alias("source")))
    .orderBy("order_date")
)
