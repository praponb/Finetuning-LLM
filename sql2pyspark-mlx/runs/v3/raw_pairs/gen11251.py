from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date", F.lit("order").alias("source"))
    .union(df_accounts.select("opened_date", F.lit("account").alias("source")))
)
