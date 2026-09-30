from pyspark.sql import functions as F

base = df_payments.filter((F.col("paid_at") >= "2025-05-01") & F.col("amount").isNull())

summary = (
    base
    .groupBy("order_id")
    .agg(
        F.sum(F.when(F.col("method") == "card", 1).otherwise(0)).alias("card_count"),
        F.count("amount").alias("amount_count"),
    )
)

result = (
    summary
    .filter(F.col("card_count") < 25)
    .select("order_id", "card_count")
    .orderBy("card_count", "order_id")
    .limit(20)
)
