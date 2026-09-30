from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull() & (F.col("order_date") < "2024-05-01"))
    .select("id", "amount", F.round("amount", 2).alias("amount_rounded"))
    .orderBy(F.col("amount").desc(), F.col("amount_rounded").desc())
)
