from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date_month_end", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").desc_nulls_last())

base = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-05-05"))) & (F.col("status") == "OPEN"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        F.coalesce("status", F.lit("unassigned")).alias("status_filled"),
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

enriched = (
    base
    .select(
        "id",
        "account_type",
        "status_filled",
        "opened_date_month_end",
        F.sum("balance").over(w).alias("running_balance"),
        F.cume_dist().over(w2).alias("balance_cume_dist"),
    )
)

remaining_keys = (
    enriched
    .select("account_type")
    .subtract(
        df_accounts
        .filter(F.col("opened_date").between("2023-06-01", "2024-11-14"))
        .select("account_type")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")), "inner")
    .select("s.*")
)

not_flagged = (
    reconciled
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (~F.col("ch.merchant").isin(["7-Eleven", "Grab"])), "left_semi")
)

result = (
    not_flagged
    .withColumn("running_balance_tier", F.when(F.col("running_balance") >= 388381, "LARGE").otherwise("SMALL"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date_month_end"))
    .groupingSets(
        [["running_balance_tier", "quarter_start"], ["running_balance_tier"]],
        "running_balance_tier",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("running_balance", [0.25, 0.5, 0.75]).alias("running_balance_quartiles"),
        F.percentile_approx("running_balance", [0.25, 0.5, 0.75]).alias("running_balance_quartiles_2"),
        F.percentile_approx("running_balance", 0.9).alias("running_balance_p90"),
    )
)
