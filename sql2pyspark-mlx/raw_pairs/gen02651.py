from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("balance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("balance").desc(), "id")

with_windows = (
    df_accounts
    .filter(F.col("opened_date").between("2024-08-01", "2025-05-01"))
    .select(
        "*",
        F.avg("balance").over(w).alias("running_avg_balance"),
        F.count("balance").over(w).alias("running_count_balance"),
        F.sum("balance").over(w).alias("running_sum_balance"),
        F.ntile(10).over(w2).alias("balance_bucket"),
    )
)

result = (
    with_windows
    .withColumn("running_count_balance_tier", F.when(F.col("running_count_balance") >= 15, "PREMIUM").otherwise("STANDARD"))
    .withColumn("running_avg_balance_tier", F.when(F.col("running_avg_balance") >= 89000, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_count_balance_tier", "running_avg_balance_tier"], ["running_count_balance_tier"]],
        "running_count_balance_tier",
        "running_avg_balance_tier",
    )
    .agg(
        F.round(F.avg("running_avg_balance"), 2).alias("avg_running_avg_balance"),
        F.round(F.sum("running_count_balance") / F.nullif(F.count(F.when(F.col("account_type") == "SAVINGS", 1)), F.lit(0)), 2).alias("running_count_balance_per_savings"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
    )
    .orderBy(F.col("avg_running_avg_balance").desc())
)
