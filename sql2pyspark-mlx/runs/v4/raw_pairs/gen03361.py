from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("balance").desc())
w2 = Window.partitionBy("account_type").orderBy("opened_date", "id")

with_windows = (
    df_accounts
    .select(
        "*",
        F.rank().over(w).alias("balance_rank"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    with_windows
    .withColumn("balance_rank_tier", F.when(F.col("balance_rank") >= 8, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["status", "balance_rank_tier"], ["status"]],
        "status",
        "balance_rank_tier",
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.round(F.avg("row_seq"), 2).alias("avg_row_seq"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_rows"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_count"),
        F.sum(F.when(F.col("account_type") == "SAVINGS", F.col("balance")).otherwise(0)).alias("savings_balance"),
        F.grouping("status").alias("grouping_level"),
    )
    .filter(F.col("open_rows") >= 1)
    .orderBy(F.col("closed_count").desc_nulls_first())
)
