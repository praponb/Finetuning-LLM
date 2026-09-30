from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("opened_date").asc_nulls_last())

not_flagged = (
    df_accounts
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.to_date(F.col("c.txn_time")) < "2024-05-04"), "left_anti")
)

deduped = (
    not_flagged
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "status")
)

filtered = (
    deduped
    .filter(~F.col("status").isin(["FROZEN", "OPEN"]))
    .select(
        "*",
        F.coalesce("status", F.lit("unassigned")).alias("status_filled"),
        F.coalesce("status", F.lit("DIRECT")).alias("resolved_status"),
    )
)

not_flagged_accounts = (
    filtered
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (F.col("x.is_flagged")), "left_anti")
)

result = (
    not_flagged_accounts
    .groupBy("status")
    .agg(
        F.size(F.collect_set("status")).alias("num_status"),
        F.count(F.when(F.col("status") == "OPEN", 1)).alias("open_count"),
    )
    .orderBy(F.col("num_status").asc_nulls_first())
)
