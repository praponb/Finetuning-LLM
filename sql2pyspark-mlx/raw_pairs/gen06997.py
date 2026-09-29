from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("txn_type", "merchant").orderBy("opened_date", "id")

with_dims = (
    df_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select("e.*", F.col("a.opened_date"))
)

joined = (
    with_dims
    .alias("a")
    .join(df_accounts.alias("p"), F.col("a.account_id") == F.col("p.id"), "left")
    .select(
        "a.*",
        F.col("p.status"),
        F.col("p.opened_date").alias("account_opened_date"),
        F.col("p.balance"),
    )
)

joined_transactions = (
    joined
    .alias("e")
    .join(df_accounts.alias("d"), F.col("e.account_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.balance").alias("account_balance"), F.col("d.account_type"))
)

enriched = (
    joined_transactions
    .alias("e")
    .join(df_accounts.alias("a"), F.col("e.account_id") == F.col("a.id"), "left")
    .select(
        "e.id",
        "e.account_id",
        "e.txn_type",
        "e.amount",
        "e.merchant",
        "e.opened_date",
        "e.status",
        "e.account_opened_date",
        F.col("a.balance").alias("account_balance_2"),
        F.col("a.account_type").alias("account_account_type"),
        F.col("a.opened_date").alias("account_opened_date_2"),
    )
)

metrics = (
    enriched
    .withColumn("prev_account_balance_2", F.lag("account_balance_2").over(w_hist))
    .withColumn("next_account_balance_2", F.lead("account_balance_2").over(w_hist))
    .withColumn("prev_amount", F.lag("amount").over(w_hist))
    .withColumn("next_amount", F.lead("amount").over(w_hist))
    .withColumn("seq_no", F.row_number().over(w_hist))
)

pivoted = (
    metrics
    .groupBy("account_account_type")
    .pivot("merchant", ["Lazada", "7-Eleven"])
    .agg(F.max("next_amount"))
    .select(
        "account_account_type",
        F.col("Lazada").alias("lazada"),
        F.col("7-Eleven").alias("v_7_eleven"),
    )
)

result = (
    pivoted
    .select("account_account_type", "lazada", "v_7_eleven")
    .orderBy("account_account_type")
)
