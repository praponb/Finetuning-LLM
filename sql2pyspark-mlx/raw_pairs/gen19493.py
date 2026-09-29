from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_id", "txn_type")
w2 = Window.partitionBy("account_id")
w_running = Window.partitionBy("account_id").orderBy("amount", "id")
w4 = Window.partitionBy("merchant").orderBy("account_id_min_amount", "id")
w5 = Window.partitionBy("merchant")
w6 = Window.partitionBy("merchant").orderBy("amount_share", "id")
w7 = Window.partitionBy("merchant").orderBy(F.col("next_amount_share_2").desc(), "id")

with_windows = (
    df_transactions
    .select(
        "id",
        "merchant",
        "is_flagged",
        F.min("amount").over(w).alias("account_id_min_amount"),
        F.round((F.col("amount") / F.sum("amount").over(w2)), 4).alias("amount_share"),
        F.ntile(5).over(w_running).alias("amount_bucket"),
    )
)

scored = (
    with_windows
    .filter(F.col("merchant") != "Grab")
    .select(
        "*",
        F.lead("amount_share", 1).over(w4).alias("next_amount_share"),
        F.sum("amount_bucket").over(w5).alias("merchant_sum_amount_bucket"),
        F.lead("amount_share", 1).over(w6).alias("next_amount_share_2"),
    )
)

base = (
    scored
    .filter((F.col("amount_bucket") != 3) & F.col("merchant").isNotNull())
    .withColumn("adjusted_amount_share", F.round((F.col("amount_share") * 0.9), 2))
)

latest = (
    base
    .withColumn("row_num", F.row_number().over(w7))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .select(
        "id",
        "merchant",
        "is_flagged",
        "account_id_min_amount",
        "amount_share",
        "next_amount_share",
        "merchant_sum_amount_bucket",
        "next_amount_share_2",
    )
    .orderBy(F.col("amount_share").desc(), F.col("next_amount_share").desc())
)
