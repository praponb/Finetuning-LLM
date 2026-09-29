from pyspark.sql import functions as F

valid = (
    df_payments
    .filter(F.col("status") == "FAILED")
    .withColumn("status_group", F.when(F.col("status").isin(["FAILED", "REFUNDED"]), "Group A").otherwise("Other"))
    .withColumn("method_filled", F.coalesce("method", F.lit("DIRECT")))
)

result = (
    valid
    .groupBy("method")
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.avg(F.when(F.col("method") == "paypal", F.col("amount"))).alias("paypal_avg_amount"),
        F.countDistinct("id").alias("distinct_ids"),
        F.min("paid_at").alias("first_seen"),
        F.max("paid_at").alias("ended_at"),
        F.max("paid_at").alias("last_seen"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_first())
)
