from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.1)
    .groupBy("status")
    .agg(
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
        F.max("amount").alias("max_amount"),
        F.sum(F.when(F.col("channel") == "web", F.col("amount")).otherwise(0)).alias("web_amount"),
    )
    .filter(F.col("web_amount") >= 45)
    .drop("web_amount")
)
