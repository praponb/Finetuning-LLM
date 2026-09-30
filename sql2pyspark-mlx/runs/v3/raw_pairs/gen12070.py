from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select("id", "amount", "store_id", F.round("amount", 2).alias("amount_rounded"))
)
