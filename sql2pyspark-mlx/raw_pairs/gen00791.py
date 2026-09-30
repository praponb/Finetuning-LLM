from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.month("account_opened_date"), "account_id").orderBy("account_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("account_opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy(F.month("account_opened_date"), "merchant").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_transactions
    .filter(F.col("txn_time") >= F.lit("2023-04-29").cast("date"))
    .select(
        "*",
        F.to_date("txn_time").alias("txn_time_date"),
        F.hour("txn_time").alias("txn_time_hour"),
    )
)

joined = (
    base
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.status"), F.col("b.account_type"), F.col("b.opened_date"))
)

with_dims = (
    joined
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "left")
    .filter(F.col("p.account_type") == "SAVINGS")
    .select(
        "s.id",
        "s.account_id",
        "s.merchant",
        "s.is_flagged",
        "s.txn_time_hour",
        "s.status",
        "s.opened_date",
        F.col("p.balance"),
        F.col("p.opened_date").alias("account_opened_date"),
    )
)

scored = (
    with_dims
    .withColumn("running_balance", F.sum("balance").over(w))
    .withColumn("last_known_merchant", F.last("merchant", ignorenulls=True).over(w2))
    .withColumn("running_balance_2", F.sum("balance").over(w3))
    .withColumn("first_known_merchant", F.first("merchant", ignorenulls=True).over(w4))
)

enriched = (
    scored
    .alias("a")
    .join(df_accounts.alias("p2"), F.col("a.account_id") == F.col("p2.id"), "left")
    .select("a.*", F.col("p2.status").alias("account_status"))
)

result = (
    enriched
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
        F.percentile_approx("running_balance", 0.95).alias("running_balance_p95"),
        F.round(F.sum("running_balance_2") / F.nullif(F.count(F.when(F.col("merchant") == "Amazon", 1)), F.lit(0)), 2).alias("running_balance_2_per_amazon"),
        F.sum(F.when(F.col("merchant") == "Amazon", F.col("running_balance")).otherwise(0)).alias("amazon_running_balance"),
    )
    .orderBy(F.col("amazon_running_balance").asc_nulls_first())
)
