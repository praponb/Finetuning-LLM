from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("shipping_fee") >= 35) & (F.col("amount") < 710))
    .groupBy(
        F.when(F.col("shipping_fee") >= 43, "high").when(F.col("shipping_fee") >= 9, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.sum("amount").alias("total_amount"))
    .filter(F.col("total_amount") > 6900)
)
