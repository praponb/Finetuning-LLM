from pyspark.sql import functions as F

with_dims = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("b"), F.col("e.account_id") == F.col("b.id"), "left")
    .filter(F.col("b.balance") > 36000)
    .select("e.*", F.col("b.status"), F.col("b.opened_date"))
)

result = (
    with_dims
    .groupBy(
        F.when(F.col("amount") >= 4300, "PREMIUM").otherwise("STANDARD").alias("amount_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.count_if(F.col("merchant") == "Lazada").alias("lazada_events"),
        F.max(F.when(F.col("status") == "OPEN", F.col("amount"))).alias("open_max_amount"),
        F.round(F.avg("amount"), 2).alias("avg_amount"),
        F.count(F.when(F.col("status") == "CLOSED", 1)).alias("closed_rows"),
        ((F.unix_timestamp(F.max("txn_time")) - F.unix_timestamp(F.min("txn_time"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("amount_tier").asc_nulls_last())
)
