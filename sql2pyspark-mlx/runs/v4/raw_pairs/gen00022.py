from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")
w2 = Window.orderBy("monthly_fee", "id")
w3 = Window.partitionBy("status").orderBy(F.col("start_date").desc(), "id")
w4 = Window.orderBy(F.col("row_seq").asc_nulls_last())
w5 = Window.partitionBy("customer_id", "plan").orderBy(F.col("monthly_fee").asc_nulls_first())
w6 = Window.partitionBy("status")
w7 = Window.partitionBy("customer_id")
w8 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_share").desc())
w9 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_share").desc_nulls_first())

scored = (
    df_subscriptions
    .withColumn("prev_monthly_fee", F.lag("monthly_fee", 2).over(w))
    .withColumn("monthly_fee_bucket", F.ntile(3).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

remaining_keys = (
    scored
    .select("plan", "status")
    .intersect(
        df_subscriptions
        .filter(F.col("status") != "EXPIRED")
        .select("plan", "status")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.status") == F.col("k.status")), "inner")
    .select("s.*")
)

scored_subscriptions = (
    kept_rows
    .withColumn("row_seq_percent_rank", F.percent_rank().over(w4))
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w5))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w6)), 4))
)

with_windows = (
    scored_subscriptions
    .withColumn("monthly_fee_dense_rank_share", F.round((F.col("monthly_fee_dense_rank") / F.sum("monthly_fee_dense_rank").over(w7)), 4))
    .withColumn("monthly_fee_share_cume_dist", F.cume_dist().over(w8))
    .withColumn("monthly_fee_share_dense_rank", F.dense_rank().over(w9))
)

result = (
    with_windows
    .select(
        "customer_id",
        "end_date",
        "status",
        "prev_monthly_fee",
        "monthly_fee_bucket",
        "row_seq",
        "row_seq_percent_rank",
        "monthly_fee_dense_rank",
        "monthly_fee_share",
    )
)
