from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(21, 22) | (F.col("channel") == "mobile"))
    .groupBy(
        F.when(F.col("shipping_fee") >= 40, "high").when(F.col("shipping_fee") >= 11, "mid").otherwise("low").alias("shipping_fee_bucket"),
    )
    .agg(F.count("*").alias("row_count"))
    .orderBy(F.col("shipping_fee_bucket").desc(), F.col("row_count").desc())
)
