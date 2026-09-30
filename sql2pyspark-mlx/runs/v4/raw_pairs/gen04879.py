from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("amount").desc(), "id")
w2 = Window.partitionBy("account_id", "account_type").orderBy(F.col("amount_filled").desc(), "id")
w3 = Window.partitionBy("account_id")
w4 = Window.partitionBy("account_id", "txn_type").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= "2024-02-25"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.coalesce("amount", F.lit(0)).alias("amount_filled"),
        F.when(F.col("is_flagged"), "Yes").otherwise("No").alias("is_flagged_label"),
        F.coalesce("merchant", F.lit("N/A")).alias("resolved_merchant"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.account_type"), F.col("p.status"), F.col("p.opened_date"))
)

scored = (
    enriched
    .withColumn("amount_bucket", F.ntile(10).over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("account_id_avg_amount", F.avg("amount").over(w3))
    .withColumn("amount_avg_3", F.avg("amount").over(w4))
)

result = (
    scored
    .groupBy("merchant")
    .pivot("txn_type", ["DEBIT", "CREDIT"])
    .agg(F.sum("row_seq").alias("total"))
    .select("merchant", F.col("DEBIT").alias("debit"), F.col("CREDIT").alias("credit"))
)
