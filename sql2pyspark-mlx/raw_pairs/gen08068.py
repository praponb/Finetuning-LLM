from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id").orderBy(F.col("txn_time_quarter").desc(), "id")
w2 = Window.partitionBy("account_id", "status")
w3 = Window.partitionBy("status", "account_id").orderBy("row_num", "id")
w4 = Window.orderBy(F.col("row_num").desc(), "id")
w5 = Window.partitionBy("account_id").orderBy("amount", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("account_id_min_amount")

filtered = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("txn_time") >= F.lit("2023-06-12").cast("date")))
    .withColumn("txn_type_group", F.when(F.col("txn_type").isin(["DEBIT", "CREDIT"]), "Core").otherwise("Other"))
    .withColumn("txn_time_quarter", F.quarter("txn_time"))
    .withColumn("resolved_merchant", F.coalesce("merchant", F.lit("DIRECT")))
)

with_dims = (
    filtered
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "inner")
    .select(
        "a.id",
        "a.account_id",
        "a.amount",
        "a.txn_type_group",
        "a.txn_time_quarter",
        F.col("b.status"),
    )
)

ranked = (
    with_dims
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    ranked
    .filter(F.col("account_id").isin([1, 12, 27]))
    .select(
        "id",
        "account_id",
        "amount",
        F.min("amount").over(w2).alias("account_id_min_amount"),
        F.lag("amount", 1).over(w3).alias("prev_amount"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

enriched_transactions = (
    enriched
    .withColumn("last7_sum_prev_amount", F.sum("prev_amount").over(w5))
    .withColumn("last7_avg_prev_amount", F.avg("prev_amount").over(w5))
    .withColumn("last7_count_prev_amount", F.count("prev_amount").over(w5))
    .withColumn("account_id_min_amount_rank", F.rank().over(w6))
)

result = (
    enriched_transactions
    .groupBy(
        F.when(F.col("account_id_min_amount") >= 3938, "HIGH").otherwise("LOW").alias("account_id_min_amount_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("account_id").alias("distinct_account_ids"),
    )
)
