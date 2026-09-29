from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy("balance", "id")
w2 = Window.partitionBy(F.month("opened_date"), "customer_id").orderBy(F.col("balance").desc(), "id")
w3 = Window.partitionBy("customer_id").orderBy("next_balance")

valid = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-02-27").cast("date")) & F.col("status").isNotNull())
    .withColumn("balance_band", F.when(F.col("balance") >= 64000, "High").when(F.col("balance") >= 31000, "Medium").otherwise("Low"))
    .withColumn("resolved_status", F.coalesce("status", F.lit("N/A")))
)

with_windows = (
    valid
    .select(
        "id",
        "customer_id",
        "account_type",
        "resolved_status",
        F.lead("balance").over(w_grp).alias("next_balance"),
        F.row_number().over(w_grp).alias("row_seq"),
        F.ntile(5).over(w2).alias("balance_bucket"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_transactions.alias("ch"), (F.col("ch.account_id") == F.col("s.id")) & (F.col("ch.account_id").isin([9, 11, 14, 26])), "left_anti")
)

latest = (
    eligible
    .withColumn("latest_rank", F.rank().over(w3))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = latest.select("id", "customer_id", "account_type", "next_balance", "balance_bucket")
