from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.year("order_date") >= 2025) & F.col("channel").isNotNull())
    .groupBy(
        F.when(F.col("shipping_fee") >= 40, "high").when(F.col("shipping_fee") >= 10, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.max("amount").alias("max_amount"))
    .filter(F.col("max_amount") > 990)
)
