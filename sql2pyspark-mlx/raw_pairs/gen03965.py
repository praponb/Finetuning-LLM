from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy(F.col("balance").asc_nulls_first())
w3 = Window.partitionBy(F.year("opened_date"), "customer_id").orderBy("running_balance", "id")
w_grp = Window.orderBy("balance_rank", "id")
w_running = Window.partitionBy(F.year("opened_date"), "customer_id").orderBy("running_balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

enriched = (
    df_accounts
    .alias("src")
    .join(df_customers.alias("b"), F.col("src.customer_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.name"), F.col("b.city"))
)

enriched_accounts = (
    enriched
    .select(
        "id",
        "customer_id",
        "opened_date",
        "name",
        F.sum("balance").over(w).alias("running_balance"),
        F.rank().over(w2).alias("balance_rank"),
    )
)

scored = (
    enriched_accounts
    .select(
        "id",
        "customer_id",
        "opened_date",
        F.lag("running_balance", 2).over(w3).alias("prev_running_balance"),
        F.row_number().over(w_grp).alias("event_seq"),
        F.sum("balance_rank").over(w_running).alias("running_balance_rank"),
    )
)

remaining_keys = (
    scored
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["FROZEN", "CLOSED"]))
        .select("id")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .withColumn("running_balance_rank_tier", F.when(F.col("running_balance_rank") >= 32, "PREMIUM").otherwise("STANDARD"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "opened_date"))
    .cube("running_balance_rank_tier", "quarter_start")
    .agg(
        F.min("prev_running_balance").alias("min_prev_running_balance"),
        F.min("prev_running_balance").alias("min_prev_running_balance_2"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.grouping("running_balance_rank_tier").alias("grouping_level"),
    )
    .orderBy(F.col("min_prev_running_balance_2").desc())
)
