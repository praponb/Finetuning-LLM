from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("amount") >= 1560, "high").when(F.col("amount") >= 750, "mid").otherwise("low").alias("amount_bucket"),
        "customer_id",
    )
    .agg(F.round(F.stddev("discount"), 2).alias("stddev_discount"))
)
