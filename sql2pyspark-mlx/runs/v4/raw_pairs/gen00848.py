from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy(F.col("monthly_fee").desc_nulls_first())
w2 = Window.partitionBy("account_type").orderBy("latest_rank", "id", "subscription_id")
w3 = Window.partitionBy("account_type").orderBy("opened_date", "id", "subscription_id")
w4 = Window.partitionBy("subscription_plan").orderBy(F.col("monthly_fee").desc(), "id", "subscription_id", "subscription_id_2")

attributed = (
    df_accounts
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") <= F.date_add(F.col("sub.end_date"), 10)), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan"),
        F.col("sub.monthly_fee"),
    )
)

deduped = (
    attributed
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.txn_time") >= "2025-08-25"), "left_semi")
)

with_windows = (
    eligible
    .select(
        "*",
        F.lag("monthly_fee", 1).over(w2).alias("prev_monthly_fee"),
        F.lead("monthly_fee", 1).over(w3).alias("next_monthly_fee"),
    )
)

attributed_accounts = (
    with_windows
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & (F.col("src.opened_date") >= F.col("sub.start_date")) & (F.col("src.opened_date") < F.col("sub.end_date") + F.expr("INTERVAL 14 DAYS")), "left")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id_2"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

latest = (
    attributed_accounts
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
)

wide = (
    latest
    .groupBy("status")
    .pivot("account_type", ["LOAN", "SAVINGS", "CHECKING"])
    .agg(F.sum("subscription_id").alias("total"))
)

result = (
    wide
    .select(
        "status",
        "LOAN",
        "SAVINGS",
        "CHECKING",
        F.when(F.col("status").rlike("^(OPEN|FROZEN)$"), "GROUP_B").otherwise("EXTENDED").alias("status_zone"),
    )
)
