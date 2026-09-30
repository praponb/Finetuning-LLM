from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant", "status").orderBy("amount", "id").rowsBetween(-6, -1)
w2 = Window.partitionBy("account_status").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-7776000, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy("amount_avg_90d", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("account_status", "account_type").orderBy("amount_int_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("account_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("account_status").orderBy("amount_int_avg_6_prior", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.partitionBy("txn_type").orderBy("cumulative_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy(F.year("opened_date"), "txn_type").orderBy(F.col("amount_int_avg_6_prior").desc(), "id")
w9 = Window.partitionBy("account_type", "txn_type").orderBy(F.col("amount_avg_5_prior").desc_nulls_last())

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "*",
        F.col("amount").cast("int").alias("amount_int"),
        F.coalesce("merchant", F.lit("UNKNOWN")).alias("merchant_clean"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_accounts.alias("p"), F.col("e.account_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.status"), F.col("p.account_type"))
)

enriched_transactions = (
    enriched
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .select(
        "s.*",
        F.col("a.account_type").alias("account_account_type"),
        F.col("a.opened_date"),
        F.col("a.status").alias("account_status"),
    )
)

enriched_transactions_2 = (
    enriched_transactions
    .filter(F.col("is_flagged"))
    .select(
        "id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        "account_type",
        "account_account_type",
        "opened_date",
        "account_status",
        F.avg("amount_int").over(w).alias("amount_int_avg_6_prior"),
        F.avg("amount").over(w2).alias("amount_avg_90d"),
    )
)

enriched_transactions_3 = (
    enriched_transactions_2
    .select(
        "*",
        F.avg("amount").over(w3).alias("amount_avg_5_prior"),
        F.sum("amount").over(w4).alias("cumulative_amount"),
    )
)

enriched_transactions_4 = (
    enriched_transactions_3
    .withColumn("running_avg_amount_int_avg_6_prior", F.avg("amount_int_avg_6_prior").over(w_recent))
    .withColumn("running_sum_amount_int_avg_6_prior", F.sum("amount_int_avg_6_prior").over(w_recent))
    .withColumn("running_count_amount_int_avg_6_prior", F.count("amount_int_avg_6_prior").over(w_recent))
    .withColumn("amount_avg_5", F.avg("amount").over(w6))
)

metrics = (
    enriched_transactions_4
    .withColumn("running_amount_avg_90d", F.sum("amount_avg_90d").over(w7))
    .withColumn("amount_int_avg_6_prior_bucket", F.ntile(3).over(w8))
    .withColumn("amount_avg_5_prior_cume_dist", F.cume_dist().over(w9))
)

pivoted = (
    metrics
    .groupBy("account_type")
    .pivot("txn_type", ["CREDIT", "DEBIT"])
    .agg(F.sum("amount_avg_90d"))
    .select(
        "account_type",
        F.col("CREDIT").alias("credit"),
        F.col("DEBIT").alias("debit"),
    )
)

result = (
    pivoted
    .select(
        "account_type",
        "credit",
        F.when(F.col("account_type").rlike("^(CHECKING|SAVINGS)$"), "OTHER_ZONE").when(F.col("account_type").rlike("^(CREDIT|LOAN)$"), "PRIORITY").otherwise("GROUP_A").alias("account_type_zone"),
    )
    .orderBy(F.col("credit").desc_nulls_last())
)
