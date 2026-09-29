from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("plan") != "enterprise") & ~F.col("status").isin(["EXPIRED", "PAUSED"]))
    .withColumn("monthly_fee_filled", F.coalesce("monthly_fee", F.lit(0)))
    .withColumn("status_title", F.initcap(F.trim("status")))
)
