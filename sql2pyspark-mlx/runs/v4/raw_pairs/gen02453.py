from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("amount", "id")
w2 = Window.partitionBy("merchant").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy("amount_sum_7d", "id")
w4 = Window.partitionBy("txn_type").orderBy("amount", "id").rowsBetween(-5, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

scored = (
    cleaned
    .select(
        "*",
        F.ntile(4).over(w).alias("amount_bucket"),
        F.sum("amount").over(w2).alias("amount_sum_7d"),
    )
)

filtered = (
    scored
    .filter((F.col("txn_time") >= F.lit("2023-09-21").cast("date")) & (F.col("account_id") == 24))
    .select(
        "id",
        "txn_type",
        "amount",
        "txn_time",
        "is_flagged_label",
        "amount_sum_7d",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label_2"),
    )
)

scored_transactions = (
    filtered
    .withColumn("amount_sum_7d_bucket", F.ntile(10).over(w3))
    .withColumn("is_flagged_flag_avg_5", F.avg("is_flagged_flag").over(w4))
)

summary = (
    scored_transactions
    .withColumn("year_start", F.date_trunc("YEAR", "txn_time"))
    .groupingSets(
        [["txn_type", "year_start"], ["txn_type"], []],
        "txn_type",
        "year_start",
    )
    .agg(
        F.count(F.when(F.col("txn_type") == "FEE", 1)).alias("fee_rows"),
        F.count(F.when(F.col("txn_type") == "CREDIT", 1)).alias("credit_count"),
        F.percentile_approx("amount_sum_7d_bucket", 0.5).alias("amount_sum_7d_bucket_p50"),
        F.grouping("txn_type").alias("grouping_id"),
    )
)

result = (
    summary
    .select(
        "txn_type",
        "year_start",
        "fee_rows",
        "credit_count",
        "amount_sum_7d_bucket_p50",
        "grouping_id",
        F.when(F.col("txn_type").rlike("^(REFUND|DEBIT)$"), "OTHER_ZONE").when(F.col("txn_type").rlike("^(CREDIT|FEE)$"), "EXTENDED").otherwise("CORE").alias("txn_type_zone"),
    )
)
