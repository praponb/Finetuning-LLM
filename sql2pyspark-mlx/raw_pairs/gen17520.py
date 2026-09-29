from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("txn_time", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy(F.date_trunc("MONTH", "txn_time"), "account_id")
w3 = Window.partitionBy("txn_type")
w4 = Window.partitionBy("account_id", "txn_type")
w5 = Window.partitionBy("txn_type").orderBy(F.col("amount").desc())
w6 = Window.partitionBy("account_id")
w7 = Window.partitionBy("account_type").orderBy(F.col("txn_type_sum_amount").desc(), "id")
w8 = Window.partitionBy("account_type").orderBy(F.col("txn_type_sum_amount").desc_nulls_last())
w9 = Window.partitionBy("account_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy(F.month("txn_time"), "account_type").orderBy("amount", "id")
w11 = Window.partitionBy("account_type").orderBy(F.col("txn_time").desc(), "id")

metrics = (
    df_transactions
    .withColumn("amount_avg_5_prior", F.avg("amount").over(w))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("txn_type_sum_amount", F.sum("amount").over(w3))
    .withColumn("account_id_sum_amount", F.sum("amount").over(w4))
    .withColumn("amount_percent_rank", F.percent_rank().over(w5))
)

enriched = (
    metrics
    .alias("src")
    .join(df_accounts.alias("a"), F.col("src.account_id") == F.col("a.id"), "inner")
    .filter(F.col("a.customer_id").isin([2, 3, 34]))
    .select(
        "src.id",
        "src.account_id",
        "src.amount",
        "src.txn_time",
        "src.txn_type_sum_amount",
        "src.account_id_sum_amount",
        F.col("a.account_type"),
    )
)

metrics_transactions = (
    enriched
    .select(
        "id",
        "amount",
        "txn_time",
        "account_id_sum_amount",
        "account_type",
        F.round((F.col("account_id_sum_amount") / F.sum("account_id_sum_amount").over(w6)), 4).alias("account_id_sum_amount_share"),
        F.ntile(5).over(w7).alias("txn_type_sum_amount_bucket"),
        F.cume_dist().over(w8).alias("txn_type_sum_amount_cume_dist"),
        F.sum("account_id_sum_amount").over(w9).alias("cumulative_account_id_sum_amount"),
        F.lag("account_id_sum_amount", 1).over(w10).alias("prev_account_id_sum_amount"),
    )
)

deduped = (
    metrics_transactions
    .withColumn("rn", F.row_number().over(w11))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "amount",
        "txn_time",
        "account_type",
        "account_id_sum_amount_share",
        "prev_account_id_sum_amount",
        "rn",
        F.when(F.col("account_type").rlike("^(LOAN|CREDIT)$"), "OTHER_ZONE").when(F.col("account_type").rlike("^(SAVINGS|CHECKING)$"), "PRIORITY").otherwise("CORE").alias("account_type_zone"),
    )
    .orderBy(F.col("rn").desc_nulls_last())
)
