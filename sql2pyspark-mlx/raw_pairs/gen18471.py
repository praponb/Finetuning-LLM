from pyspark.sql import functions as F

filtered_payments = (
    df_payments
    .filter(F.hour("paid_at").between(10, 13))
    .withColumn("amount_int", F.col("amount").cast("int"))
)

summary = (
    filtered_payments
    .groupBy(F.hour("paid_at").alias("paid_hour"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("method") == "card", 1).otherwise(0))) / F.count("*")), 1).alias("pct_card"),
    )
)

result = (
    summary
    .select("paid_hour", "pct_card")
    .orderBy(F.col("pct_card").desc(), "paid_hour")
)
