from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("merchant").orderBy("is_flagged_flag", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_id", "merchant").orderBy("txn_time_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.to_date("txn_time").alias("txn_time_date"),
    )
)

metrics = (
    cleaned
    .select(
        "*",
        F.avg("amount").over(w).alias("running_avg_amount"),
        F.sum("amount").over(w).alias("running_sum_amount"),
        F.max("amount").over(w).alias("running_max_amount"),
        F.min("amount").over(w).alias("running_min_amount"),
    )
)

joined = (
    metrics
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .filter(F.col("p.customer_id").isNotNull())
    .select("s.*", F.col("p.opened_date"))
)

enriched = (
    joined
    .select(
        "*",
        F.count("amount").over(w2).alias("running_count_amount"),
        F.max("amount").over(w2).alias("running_max_amount_2"),
        F.avg("amount").over(w2).alias("running_avg_amount_2"),
        F.last("amount", ignorenulls=True).over(w3).alias("last_known_amount"),
    )
)

result = (
    enriched
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "txn_time_date",
        "opened_date",
        "running_max_amount_2",
        "running_avg_amount_2",
        "last_known_amount",
        F.when(F.col("merchant").rlike("^(7-Eleven|Amazon)$"), "EXTENDED").when(F.col("merchant").rlike("^(Grab|Lazada)$"), "GROUP_B").otherwise("GROUP_A").alias("merchant_zone"),
    )
    .orderBy("id", "account_id")
    .limit(10)
)
