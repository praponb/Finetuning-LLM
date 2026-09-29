from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "txn_type").orderBy("balance")
w2 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("account_type").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("opened_date", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("txn_type").orderBy("balance", "id")
w6 = Window.orderBy("balance", "id").rowsBetween(-5, -1)
w7 = Window.partitionBy("account_type").orderBy(F.col("prev_balance").desc(), "id")

valid = (
    df_transactions
    .filter((F.col("amount") >= 2300) & (F.col("merchant").eqNullSafe(F.lit("Amazon"))))
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "merchant",
        "is_flagged",
        F.round("amount", 2).alias("amount_rounded"),
        F.coalesce("merchant", F.lit("NONE")).alias("merchant_filled"),
    )
)

with_dims = (
    valid
    .alias("src")
    .join(df_accounts.alias("p"), F.col("src.account_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_type",
        "src.merchant",
        "src.is_flagged",
        F.col("p.account_type"),
        F.col("p.balance"),
        F.col("p.opened_date"),
    )
)

deduped = (
    with_dims
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.first("account_type", ignorenulls=True).over(w2).alias("first_known_account_type"),
        F.first("account_type", ignorenulls=True).over(w3).alias("first_known_account_type_2"),
        F.avg("balance").over(w_hist).alias("balance_avg_3_prior"),
    )
)

metrics = (
    enriched
    .filter(F.col("opened_date").between("2025-05-01", "2025-06-01"))
    .select(
        "id",
        "account_id",
        "txn_type",
        "merchant",
        "is_flagged",
        "account_type",
        "first_known_account_type",
        F.lag("balance", 2).over(w5).alias("prev_balance"),
        F.avg("balance_avg_3_prior").over(w6).alias("balance_avg_3_prior_avg_5_prior"),
    )
)

deduped_transactions = (
    metrics
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "merchant", "account_type", "prev_balance")
)

base = (
    deduped_transactions
    .filter(F.col("account_type").isNotNull())
    .withColumn("account_type_merchant", F.concat("account_type", F.lit(", "), "merchant"))
    .withColumn("resolved_account_type", F.coalesce("account_type", F.lit("OTHER")))
)

result = (
    base
    .select(
        "id",
        "merchant",
        "account_type",
        "account_type_merchant",
        "resolved_account_type",
    )
    .orderBy(F.col("id").desc_nulls_first())
)
