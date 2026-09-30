from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("opened_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("balance", "id")
w3 = Window.partitionBy("status").orderBy("balance", "id")
w4 = Window.partitionBy("status", "account_type").orderBy("opened_date_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("customer_id", "status").orderBy("balance")
w6 = Window.orderBy(F.col("balance_percent_rank").desc(), "id")
w7 = Window.orderBy("cumulative_balance", "id")

filtered = (
    df_accounts
    .filter(F.col("balance") >= 60000)
    .select(
        "*",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

scored = (
    filtered
    .filter(F.col("balance") > 68000)
    .select(
        "id",
        "balance",
        "opened_date",
        "opened_date_plus_months",
        "resolved_status",
        F.last("balance", ignorenulls=True).over(w_running).alias("last_known_balance"),
        F.lead("balance", 2).over(w2).alias("next_balance"),
        F.row_number().over(w3).alias("row_seq"),
        F.sum("balance").over(w4).alias("cumulative_balance"),
        F.percent_rank().over(w_recent).alias("balance_percent_rank"),
    )
)

kept = (
    scored
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.hour(F.col("c.txn_time")).between(7, 13)), "left_anti")
)

enriched = (
    kept
    .select(
        "id",
        "balance",
        "opened_date_plus_months",
        "resolved_status",
        "row_seq",
        "cumulative_balance",
        "balance_percent_rank",
        F.row_number().over(w6).alias("row_seq_2"),
        F.lead("balance_percent_rank", 2).over(w7).alias("next_balance_percent_rank"),
    )
)

result = (
    enriched
    .withColumn("next_balance_percent_rank_tier", F.when(F.col("next_balance_percent_rank") >= 0.7, "HIGH").otherwise("LOW"))
    .withColumn("week_start", F.date_trunc("week", "opened_date_plus_months"))
    .groupingSets(
        [["next_balance_percent_rank_tier", "week_start"], ["next_balance_percent_rank_tier"]],
        "next_balance_percent_rank_tier",
        "week_start",
    )
    .agg(
        F.percentile_approx("next_balance_percent_rank", [0.25, 0.5, 0.75]).alias("next_balance_percent_rank_quartiles"),
        F.percentile_approx("next_balance_percent_rank", 0.5).alias("next_balance_percent_rank_p50"),
        F.round(F.avg("next_balance_percent_rank"), 2).alias("avg_next_balance_percent_rank"),
        F.percentile_approx("balance", 0.5).alias("balance_p50"),
        F.sum("balance_percent_rank").alias("total_balance_percent_rank"),
        F.max("balance_percent_rank").alias("max_balance_percent_rank"),
        F.grouping_id().alias("grouping_id"),
    )
)
