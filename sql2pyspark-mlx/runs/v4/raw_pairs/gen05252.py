from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy("txn_time", "id")
w2 = Window.partitionBy("merchant").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("merchant", "txn_type").orderBy("txn_time", "id")
w4 = Window.partitionBy("txn_type", "account_id").orderBy(F.col("amount").desc(), "id")
w_grp = Window.partitionBy("merchant").orderBy("rank_in_group", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("txn_type").orderBy("island_id", "id")
w7 = Window.orderBy("amount", "id")
w8 = Window.partitionBy("account_id", "merchant").orderBy(F.col("amount").desc_nulls_last())

flagged = (
    df_transactions
    .withColumn("new_group_flag", F.when(F.lag("txn_time").over(w).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

islands = flagged.withColumn("island_id", F.sum("new_group_flag").over(w2))

ranked = (
    islands
    .withColumn("rank_in_group", F.row_number().over(w3))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .select(
        "*",
        F.ntile(4).over(w4).alias("amount_bucket"),
        F.avg("amount").over(w_grp).alias("amount_avg_2_prior"),
    )
)

scored = (
    metrics
    .withColumn("prev_amount_bucket", F.lag("amount_bucket", 1).over(w6))
    .withColumn("next_island_id", F.lead("island_id", 1, 0).over(w7))
    .withColumn("amount_dense_rank", F.dense_rank().over(w8))
)

matched_keys = (
    scored
    .select("id")
    .exceptAll(
        df_transactions
        .filter(F.hour("txn_time").between(6, 10))
        .select("id")
    )
)

verified = (
    scored
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "id",
        "account_id",
        "txn_type",
        "amount",
        "txn_time",
        "merchant",
        "is_flagged",
        "new_group_flag",
        "rank_in_group",
    )
)
