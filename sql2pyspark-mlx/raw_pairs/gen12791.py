from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("txn_type").orderBy(F.col("amount").desc_nulls_first())
w2 = Window.partitionBy("account_id").orderBy("txn_time", "id")
w3 = Window.partitionBy("account_id").orderBy("txn_time", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("rank_in_group").desc())

cleaned = (
    df_transactions
    .filter(F.col("merchant").isNotNull() & (F.col("merchant").eqNullSafe(F.lit("Grab"))))
    .select(
        "*",
        F.round((F.col("amount") * 0.85), 2).alias("adjusted_amount"),
        F.when(F.col("amount") >= 4000, "High").when(F.col("amount") >= 1300, "Medium").otherwise("Low").alias("amount_band"),
        F.coalesce("merchant", F.lit("N/A")).alias("merchant_filled"),
    )
)

deduped = (
    cleaned
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
)

gaps = (
    deduped
    .withColumn("is_new_session", F.when(F.lag("txn_time").over(w2).isNull() | (F.col("txn_time") > F.lag("txn_time").over(w2) + F.expr("INTERVAL 30 DAYS")), 1).otherwise(0))
)

sessionized = gaps.withColumn("island_id", F.sum("is_new_session").over(w3))

joined = (
    sessionized
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.opened_date"), F.col("p.account_type"))
)

deduped_transactions = (
    joined
    .withColumn("latest_rank", F.rank().over(w4))
    .filter(F.col("latest_rank") == 1)
)

with_dims = (
    deduped_transactions
    .alias("src")
    .join(df_accounts.alias("d"), F.col("src.account_id") == F.col("d.id"), "left")
    .filter(F.col("d.opened_date") >= "2024-10-01")
    .select(
        "src.id",
        "src.account_id",
        "src.txn_time",
        "src.is_flagged",
        "src.adjusted_amount",
        "src.merchant_filled",
        "src.rank_in_group",
        "src.account_type",
        "src.latest_rank",
        F.col("d.opened_date").alias("account_opened_date"),
        F.col("d.status"),
        F.col("d.account_type").alias("account_account_type"),
    )
)

by_type = (
    with_dims
    .groupBy("account_type")
    .pivot("status", ["FROZEN", "OPEN"])
    .agg(F.avg("adjusted_amount").alias("total"), F.count(F.lit(1)).alias("n"))
)

result = (
    by_type
    .select(
        "account_type",
        "FROZEN_total",
        "OPEN_total",
        F.when(F.col("account_type").rlike("^(CHECKING|LOAN)$"), "GROUP_B").otherwise("PRIORITY").alias("account_type_zone"),
    )
    .orderBy(F.col("FROZEN_total").asc_nulls_first())
)
