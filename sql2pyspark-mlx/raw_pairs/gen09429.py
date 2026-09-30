from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("p.order_id"), "outer")
    .groupBy(
        F.when(F.col("o.amount") >= 1630, "high").when(F.col("o.amount") >= 770, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.round(F.stddev(F.col("p.amount")), 2).alias("stddev_amount"))
    .orderBy("amount_bucket", F.col("stddev_amount").desc())
)
