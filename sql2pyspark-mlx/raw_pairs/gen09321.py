from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("account_type").orderBy("amount", "id")
w_grp = Window.partitionBy("account_type").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_type").orderBy("running_avg_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("account_type")
w6 = Window.partitionBy("account_type").orderBy("running_max_amount", "id")

with_latest = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.account_type", F.col("c.amount"))
)

deduped = (
    with_latest
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    deduped
    .withColumn("running_avg_amount", F.avg("amount").over(w_grp))
    .withColumn("running_min_amount", F.min("amount").over(w_grp))
    .withColumn("running_sum_amount", F.sum("amount").over(w_grp))
    .withColumn("running_max_amount", F.max("amount").over(w_grp))
)

kept = (
    metrics
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant") != "Lazada"), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.txn_time") >= "2024-05-23"), "left_anti")
)

result = (
    eligible
    .filter(F.col("amount") >= 1500)
    .select(
        "id",
        "rank_in_group",
        "running_sum_amount",
        F.first("amount", ignorenulls=True).over(w4).alias("first_known_amount"),
        F.round((F.col("running_max_amount") / F.sum("running_max_amount").over(w5)), 4).alias("running_max_amount_share"),
        F.sum("running_sum_amount").over(w_grp).alias("running_running_sum_amount"),
        F.ntile(5).over(w6).alias("running_max_amount_bucket"),
    )
)
