from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "channel",
        F.when(F.col("amount") >= 1490, "high").when(F.col("amount") >= 490, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"))
)
