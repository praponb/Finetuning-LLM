from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter((F.col("monthly_fee") > 130) & ~F.col("plan").isin(["premium", "basic", "standard"]) & F.col("status").isin(["CANCELLED", "EXPIRED", "PAUSED"]))
    .groupBy("plan")
    .agg(
        (F.max("monthly_fee") - F.min("monthly_fee")).alias("monthly_fee_range"),
        F.count(F.when(F.col("status") == "EXPIRED", 1)).alias("expired_count"),
    )
    .filter(F.col("monthly_fee_range") < 39)
    .orderBy(F.col("plan").desc())
    .limit(5)
)
