from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w2 = Window.partitionBy("txn_type").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("txn_type").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w4 = Window.partitionBy("txn_type").orderBy("opened_date", "id")
w5 = Window.partitionBy("status").orderBy("account_balance")

base = (
    df_transactions
    .filter((F.col("txn_time") > F.to_date(F.lit("2023-06-30"))) & F.col("merchant").isNotNull())
    .select(
        "*",
        F.col("is_flagged").cast("int").alias("is_flagged_flag"),
        F.col("is_flagged").cast("int").alias("is_flagged_flag_2"),
        F.coalesce("merchant", F.lit("DIRECT")).alias("resolved_merchant"),
    )
)

flagged = (
    base
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 90 DAYS")), 1).otherwise(0))
)

sessionized = flagged.withColumn("burst_id", F.sum("new_group_flag").over(w2))

joined = (
    sessionized
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "inner")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.amount",
        "a.txn_time",
        "a.is_flagged_flag",
        "a.is_flagged_flag_2",
        "a.resolved_merchant",
        "a.new_group_flag",
        F.col("p2.status"),
        F.col("p2.balance"),
        F.col("p2.account_type"),
    )
)

with_dims = (
    joined
    .alias("s")
    .join(df_accounts.alias("a"), F.col("s.account_id") == F.col("a.id"), "left")
    .filter(F.col("a.opened_date").between("2023-08-25", "2023-09-01"))
    .select(
        "s.id",
        "s.account_id",
        "s.txn_type",
        "s.amount",
        "s.is_flagged_flag_2",
        "s.status",
        "s.account_type",
        F.col("a.balance").alias("account_balance"),
        F.col("a.opened_date"),
        F.col("a.account_type").alias("account_account_type"),
    )
)

latest = (
    with_dims
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "account_id",
        "txn_type",
        "status",
        "account_type",
        "account_balance",
        "opened_date",
    )
)

result = (
    latest
    .select(
        "id",
        "account_id",
        "account_type",
        "account_balance",
        "opened_date",
        F.lead("account_balance", 1).over(w4).alias("next_account_balance"),
        F.cume_dist().over(w5).alias("account_balance_cume_dist"),
    )
)
