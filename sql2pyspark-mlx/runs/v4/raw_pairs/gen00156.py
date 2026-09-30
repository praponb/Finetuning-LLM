from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("account_type", "customer_id").orderBy("balance_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("customer_id").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("account_type").orderBy(F.col("balance").desc())
w4 = Window.partitionBy("customer_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("customer_id", "account_type").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_accounts
    .filter(F.col("opened_date") > "2024-08-01")
    .select(
        "id",
        "customer_id",
        "account_type",
        "balance",
        F.ceil("balance").alias("balance_whole"),
    )
)

eligible = (
    valid
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_type").isin(["FEE", "CREDIT"])), "left_semi")
)

metrics = (
    eligible
    .withColumn("cumulative_balance_whole", F.sum("balance_whole").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_hist))
)

scored = (
    metrics
    .withColumn("balance_cume_dist", F.cume_dist().over(w3))
    .withColumn("running_seq_no", F.sum("seq_no").over(w4))
    .withColumn("last_known_balance", F.last("balance", ignorenulls=True).over(w5))
)

summary = (
    scored
    .groupBy(
        F.when(F.col("balance_cume_dist") >= 0.82, "LARGE").otherwise("SMALL").alias("balance_cume_dist_tier"),
        "seq_no",
    )
    .agg(
        F.sort_array(F.collect_set("account_type")).alias("account_type_list"),
        F.max("balance_whole").alias("max_balance_whole"),
        F.round(F.sum("balance_cume_dist") / F.nullif(F.count(F.when(F.col("account_type") == "SAVINGS", 1)), F.lit(0)), 2).alias("balance_cume_dist_per_savings"),
        F.round(F.avg("balance_whole"), 2).alias("avg_balance_whole"),
    )
    .filter(F.col("avg_balance_whole") < 0)
)

result = (
    summary
    .select(
        "balance_cume_dist_tier",
        "seq_no",
        "account_type_list",
        "balance_cume_dist_per_savings",
        "avg_balance_whole",
    )
    .orderBy(F.col("seq_no").desc_nulls_last())
)
