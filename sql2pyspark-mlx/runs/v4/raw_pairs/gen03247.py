from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("is_open", "id")
w_hist = Window.orderBy(F.col("balance_filled").desc(), "id")
w3 = Window.orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("next_is_open", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

filtered = (
    df_accounts
    .filter(F.col("opened_date") >= "2025-01-26")
    .select(
        "*",
        F.when(F.col("balance") >= 65000, "High").when(F.col("balance") >= 20000, "Medium").otherwise("Low").alias("balance_tier"),
    )
)

filtered_accounts = (
    filtered
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "balance",
        "opened_date",
        "balance_tier",
        F.when(F.col("status") == "OPEN", 1).otherwise(0).alias("is_open"),
        F.coalesce("balance", F.lit(0)).alias("balance_filled"),
        F.coalesce("status", F.lit("UNKNOWN")).alias("resolved_status"),
    )
)

with_windows = (
    filtered_accounts
    .withColumn("next_is_open", F.lead("is_open", 1).over(w))
    .withColumn("row_seq", F.row_number().over(w_hist))
    .withColumn("balance_filled_bucket", F.ntile(5).over(w_hist))
)

enriched = (
    with_windows
    .select(
        "*",
        F.last("row_seq", ignorenulls=True).over(w3).alias("last_known_row_seq"),
        F.avg("is_open").over(w4).alias("is_open_avg_3"),
    )
)

eligible = (
    enriched
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= F.col("s.opened_date")), "left_anti")
)

latest_match = (
    eligible
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.txn_type"))
)

not_flagged = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") <= F.col("s.opened_date")), "left_semi")
)

result = (
    not_flagged
    .select("id", "txn_type", "balance_filled", "balance")
    .unpivot(["id", "txn_type"], ["balance_filled", "balance"], "kpi", "val")
    .filter(F.col("val").isNotNull())
)
