from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("shipping_fee") >= 39, "high").when(F.col("shipping_fee") >= 5, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.min("amount").alias("min_amount"),
    )
    .filter(F.col("min_amount") < 1390)
    .orderBy(F.col("min_amount").desc(), "shipping_fee_bucket")
    .limit(3)
)
