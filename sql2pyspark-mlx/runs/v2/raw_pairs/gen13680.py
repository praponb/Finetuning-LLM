from pyspark.sql import functions as F

src = (
    df_payments
    .filter(F.col("method") == "card")
    .withColumn("paid_at_weekofyear", F.weekofyear("paid_at"))
)

summary = (
    src
    .groupBy(F.to_date("paid_at").alias("paid_day"), "method")
    .agg(
        F.max("amount").alias("max_amount"),
        F.round(((100 * F.sum(F.when(F.col("method") == "cash", 1).otherwise(0))) / F.count("*")), 1).alias("pct_cash"),
    )
)

result = (
    summary
    .filter(F.col("max_amount") >= 570)
    .select("paid_day", "method", "max_amount", "pct_cash")
    .orderBy("paid_day", F.col("pct_cash").desc())
)
