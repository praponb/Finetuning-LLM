from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.orderBy(F.col("txn_time").desc(), "id")
w3 = Window.partitionBy(F.year("opened_date"), "merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_id").orderBy(F.col("txn_time").cast("bigint")).rangeBetween(-604800, Window.currentRow)
w_hist = Window.orderBy("row_seq", "id")
w_running = Window.partitionBy(F.to_date("txn_time"), "account_type").orderBy(F.col("seq_no").desc_nulls_last())
w8 = Window.orderBy(F.col("seq_no").desc())

valid = (
    df_transactions
    .filter((F.col("txn_time") > F.lit("2023-09-17 17:00:00").cast("timestamp")) & F.col("merchant").isNotNull())
    .select(
        "id",
        "account_id",
        "txn_time",
        "merchant",
        "is_flagged",
        F.when(F.col("amount") >= 3800, "High").when(F.col("amount") >= 1100, "Medium").otherwise("Low").alias("amount_tier"),
        F.when(F.col("amount") >= 4200, "High").when(F.col("amount") >= 1200, "Medium").otherwise("Low").alias("amount_level"),
    )
)

with_dims = (
    valid
    .alias("s")
    .join(df_accounts.alias("b"), F.col("s.account_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.opened_date"), F.col("b.account_type"))
)

enriched = (
    with_dims
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("seq_no"),
        F.first("merchant", ignorenulls=True).over(w3).alias("first_known_merchant"),
        F.last("merchant", ignorenulls=True).over(w4).alias("last_known_merchant"),
    )
)

enriched_transactions = (
    enriched
    .select(
        "id",
        "account_id",
        "txn_time",
        "is_flagged",
        "amount_level",
        "account_type",
        "seq_no",
        "last_known_merchant",
        F.avg("row_seq").over(w5).alias("row_seq_avg_7d"),
        F.ntile(5).over(w_hist).alias("row_seq_bucket"),
    )
)

scored = (
    enriched_transactions
    .filter(F.col("row_seq_bucket") <= 8)
    .withColumn("seq_no_rank", F.rank().over(w_running))
    .withColumn("seq_no_dense_rank", F.dense_rank().over(w8))
)

kept_keys = (
    scored
    .select("id", "account_id")
    .subtract(
        df_transactions
        .filter(F.col("merchant") == "Starbucks")
        .select("id", "account_id")
    )
)

verified = (
    scored
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.account_id") == F.col("k.account_id")), "inner")
    .select("s.*")
)

filtered = (
    verified
    .filter(F.col("account_type").isNotNull())
    .withColumn("is_flagged_label", F.when(F.col("is_flagged"), "Yes").otherwise("No"))
)

result = (
    filtered
    .select(
        "is_flagged",
        "amount_level",
        "account_type",
        "last_known_merchant",
        "row_seq_avg_7d",
        "row_seq_bucket",
        "seq_no_rank",
        "seq_no_dense_rank",
    )
    .orderBy("row_seq_avg_7d")
)
