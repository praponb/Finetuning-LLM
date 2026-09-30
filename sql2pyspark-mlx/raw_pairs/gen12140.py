from pyspark.sql import functions as F

recent_payments = (
    df_payments
    .filter(F.hour("paid_at").between(6, 13) & (F.col("amount") >= 1000))
    .withColumn("amount_whole", F.ceil("amount"))
)

payments_stats = (
    recent_payments
    .groupBy(
        F.when(F.year("paid_at") < 2024, "before_2024").when(F.year("paid_at") == 2024, "2024").otherwise("after_2024").alias("paid_cohort"),
    )
    .agg(F.count("amount").alias("amount_count"))
)

result = payments_stats.orderBy(F.col("paid_cohort").desc())
