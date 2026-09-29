from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["partner", "mobile", "store"]))
    .groupBy(
        F.when(F.col("amount") >= 1620, "high").when(F.col("amount") >= 360, "mid").otherwise("low").alias("amount_bucket"),
        "status",
    )
    .agg((F.max("amount") - F.min("amount")).alias("amount_range"))
    .filter(F.col("amount_range") > 74)
    .select("amount_bucket", "status", "amount_range")
    .orderBy("status")
)
