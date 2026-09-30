from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.txn_time").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("merchant", "status").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("transaction_amount").desc(), "id")
w4 = Window.orderBy("amount", "id")
w5 = Window.orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("status", "customer_id").orderBy("amount", "id")
w7 = Window.partitionBy("status").orderBy(F.col("running_amount").desc(), "id")

cleaned = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-05-20"))) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "status",
        F.trim("account_type").alias("clean_account_type"),
        F.initcap(F.trim("account_type")).alias("account_type_title"),
    )
)

latest_match = (
    cleaned
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.merchant"), F.col("c.amount"))
)

with_latest = (
    latest_match
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.amount").alias("transaction_amount"))
)

enriched = (
    with_latest
    .withColumn("first_known_status", F.first("status", ignorenulls=True).over(w2))
    .withColumn("row_seq", F.row_number().over(w3))
)

with_windows = (
    enriched
    .select(
        "*",
        F.row_number().over(w4).alias("row_seq_2"),
        F.sum("amount").over(w5).alias("running_amount"),
        F.lead("amount", 1, 0).over(w6).alias("next_amount"),
    )
)

latest = (
    with_windows
    .withColumn("rank_in_group", F.row_number().over(w7))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    latest
    .groupBy("status")
    .pivot("merchant", ["Shell", "Lazada"])
    .agg(F.sum("row_seq"))
)
