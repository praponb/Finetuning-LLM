from pyspark.sql import functions as F

result = (
    df_transactions
    .withColumn("amount_tier", F.when(F.col("amount") >= 3200, "PREMIUM").otherwise("STANDARD"))
    .withColumn("activity_month", F.date_trunc("MONTH", "txn_time"))
    .groupingSets(
        [["amount_tier", "activity_month"], ["amount_tier"]],
        "amount_tier",
        "activity_month",
    )
    .agg(
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.max("txn_time").alias("last_txn_time"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
        F.countDistinct("txn_type").alias("distinct_txn_types"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("distinct_txn_types") >= 1)
    .orderBy(F.col("avg_amount").asc_nulls_first())
)
