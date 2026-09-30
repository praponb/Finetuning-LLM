from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "account_type").orderBy(F.col("opened_date").desc_nulls_last(), "id")
w_hist = Window.partitionBy("customer_id").orderBy("balance", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("customer_id").orderBy(F.col("last7_max_balance").desc(), "id")

filtered = (
    df_accounts
    .filter((F.col("opened_date") >= F.lit("2023-04-27").cast("date")) & F.col("status").isNotNull())
    .select("*", F.initcap(F.trim("account_type")).alias("account_type_title"))
)

deduped = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "customer_id", "balance")
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.account_id").isin([16, 17, 23, 24])), "left_semi")
)

enriched = (
    not_flagged
    .filter(F.col("balance") > 41000)
    .select(
        "id",
        "customer_id",
        F.max("balance").over(w_hist).alias("last7_max_balance"),
        F.avg("balance").over(w_hist).alias("last7_avg_balance"),
        F.count("balance").over(w_hist).alias("last7_count_balance"),
        F.sum("balance").over(w_hist).alias("last7_sum_balance"),
    )
)

ranked = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "last7_sum_balance")
)

matched_keys = (
    ranked
    .select("id")
    .subtract(
        df_accounts
        .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
        .select("id")
    )
)

verified = (
    ranked
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    verified
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.hour(F.col("c.txn_time")).between(7, 14)), "left_anti")
)

result = (
    kept
    .select("id", "last7_sum_balance")
    .orderBy(F.col("last7_sum_balance").desc_nulls_first())
)
