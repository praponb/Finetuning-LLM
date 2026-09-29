from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type", "status").orderBy("balance", "id")
w2 = Window.partitionBy("customer_id").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

latest = (
    df_accounts
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

ranked = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "balance")
)

with_last = (
    ranked
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.balance", F.col("c.merchant").alias("transaction_merchant"))
)

cleaned = (
    with_last
    .filter(F.col("transaction_merchant").isin(["Starbucks", "7-Eleven", "Grab"]))
    .select(
        "*",
        F.ceil("balance").alias("balance_whole"),
        F.coalesce("transaction_merchant", F.lit("unassigned")).alias("transaction_merchant_filled"),
    )
)

result = (
    cleaned
    .groupBy(
        F.when(F.col("balance") >= 85000, "HIGH").otherwise("LOW").alias("balance_tier"),
    )
    .agg(
        F.percentile_approx("balance_whole", [0.25, 0.5, 0.75]).alias("balance_whole_quartiles"),
        F.min("balance_whole").alias("min_balance_whole"),
        F.count(F.when(F.col("transaction_merchant") == "Grab", 1)).alias("grab_count"),
        F.count(F.when(F.col("transaction_merchant") == "Amazon", 1)).alias("amazon_count"),
        F.avg(F.when(F.col("transaction_merchant") == "Amazon", F.col("balance_whole"))).alias("amazon_avg_balance_whole"),
        F.count(F.when(F.col("transaction_merchant") == "Lazada", 1)).alias("lazada_count"),
    )
    .orderBy(F.col("balance_tier").asc_nulls_first())
)
