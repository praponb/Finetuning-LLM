from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("txn_type").orderBy("balance_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("balance_filled", "id")

base = (
    df_accounts
    .filter((F.col("status").eqNullSafe(F.lit("CLOSED"))) & (F.col("account_type") == "CHECKING"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "status",
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.coalesce("status", F.lit("NONE")).alias("resolved_status"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.account_type",
        "s.status",
        "s.balance_filled",
        F.col("c.txn_type"),
    )
)

kept = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.to_date(F.col("c.txn_time")) < "2023-07-01"), "left_semi")
)

enriched = (
    kept
    .filter(F.col("status") == "OPEN")
    .select(
        "id",
        "account_type",
        F.first("status", ignorenulls=True).over(w_hist).alias("first_known_status"),
        F.lag("balance_filled", 1).over(w3).alias("prev_balance_filled"),
    )
)

kept_accounts = (
    enriched
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.amount") <= 2700), "left_semi")
)

summary = (
    kept_accounts
    .groupBy(
        F.when(F.col("prev_balance_filled") >= 66, "HIGH").otherwise("LOW").alias("prev_balance_filled_tier"),
    )
    .agg(
        F.max(F.when(F.col("account_type") == "CHECKING", F.col("prev_balance_filled"))).alias("checking_max_prev_balance_filled"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("account_type")).alias("num_account_types"),
    )
)

result = (
    summary
    .select(
        "prev_balance_filled_tier",
        "checking_max_prev_balance_filled",
        "distinct_ids",
        "num_account_types",
    )
    .orderBy(F.col("num_account_types").desc(), "prev_balance_filled_tier")
)
