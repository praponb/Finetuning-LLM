from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNull())
    .groupBy(
        F.when(F.col("shipping_fee") >= 30, "high").when(F.col("shipping_fee") >= 20, "mid").otherwise("low").alias("shipping_fee_bucket"),
        "status",
    )
    .agg(F.min("amount").alias("min_amount"))
    .orderBy("status")
)
