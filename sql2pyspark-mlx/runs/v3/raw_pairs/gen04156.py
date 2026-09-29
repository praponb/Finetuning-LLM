from pyspark.sql import functions as F

result = (
    df_subscriptions
    .groupBy(F.when(F.col("status").isin(["ACTIVE", "PAUSED"]), "group_a").otherwise("other").alias("status_group"))
    .pivot("plan", ["enterprise", "basic", "premium", "standard"])
    .agg(F.sum("monthly_fee"))
)
