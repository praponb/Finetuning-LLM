from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("opened_date", "id")
w_grp = Window.orderBy(F.col("balance").asc_nulls_first())
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("account_type").orderBy(F.col("status_sum_balance").desc_nulls_last())
w6 = Window.partitionBy("account_type").orderBy(F.col("balance_rank").asc_nulls_first())

filtered = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-08-24").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date_2"),
        F.coalesce("status", F.lit("N/A")).alias("resolved_status"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        "opened_date",
        "status",
        "resolved_status",
    )
)

remaining_keys = (
    deduped
    .select("account_type", "customer_id")
    .intersect(
        df_accounts
        .filter(F.col("customer_id").isin([14, 16, 24]))
        .select("account_type", "customer_id")
    )
)

verified = (
    deduped
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.account_type") == F.col("k.account_type")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

scored = (
    verified
    .select(
        "id",
        "account_type",
        F.rank().over(w_grp).alias("balance_rank"),
        F.avg("balance").over(w3).alias("customer_id_avg_balance"),
        F.sum("balance").over(w4).alias("status_sum_balance"),
    )
)

with_windows = (
    scored
    .select(
        "id",
        "account_type",
        "balance_rank",
        "customer_id_avg_balance",
        "status_sum_balance",
        F.percent_rank().over(w5).alias("status_sum_balance_percent_rank"),
        F.percent_rank().over(w6).alias("balance_rank_percent_rank"),
    )
)

result = (
    with_windows
    .groupBy(
        "account_type",
        F.when(F.col("status_sum_balance_percent_rank") >= 0.85, "HIGH").otherwise("LOW").alias("status_sum_balance_percent_rank_tier"),
        F.when(F.col("customer_id_avg_balance") >= 86891, "LARGE").otherwise("SMALL").alias("customer_id_avg_balance_tier"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("account_type") == "CREDIT", F.col("customer_id_avg_balance")).otherwise(0)).alias("credit_customer_id_avg_balance"),
        F.count(F.when(F.col("account_type") == "CREDIT", 1)).alias("credit_rows"),
        F.count(F.when(F.col("account_type") == "SAVINGS", 1)).alias("savings_count"),
        F.avg("status_sum_balance").alias("avg_status_sum_balance"),
        F.round(F.sum("balance_rank") / F.nullif(F.count(F.when(F.col("account_type") == "SAVINGS", 1)), F.lit(0)), 2).alias("balance_rank_per_savings"),
    )
    .filter(F.col("credit_rows") >= 1)
)
