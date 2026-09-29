from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("account_type").orderBy("opened_date", "id")
w2 = Window.partitionBy("account_id").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("account_id").orderBy("opened_date", "id")
w5 = Window.orderBy("event_seq", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.partitionBy("account_type").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("account_type").orderBy("event_seq", "id")
w8 = Window.orderBy("first_known_event_seq_2", "id")
w9 = Window.partitionBy("account_type")

joined = (
    df_transactions
    .alias("a")
    .join(df_accounts.alias("b"), F.col("a.account_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.account_id",
        "a.txn_type",
        "a.amount",
        "a.is_flagged",
        F.col("b.opened_date"),
    )
)

enriched = (
    joined
    .alias("s")
    .join(df_accounts.alias("p"), F.col("s.account_id") == F.col("p.id"), "inner")
    .select(
        "s.id",
        "s.account_id",
        "s.is_flagged",
        "s.opened_date",
        F.col("p.account_type"),
    )
)

scored = (
    enriched
    .select(
        "id",
        "is_flagged",
        "account_type",
        F.row_number().over(w_recent).alias("event_seq"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.last("account_type", ignorenulls=True).over(w2).alias("last_known_account_type"),
        F.first("account_type", ignorenulls=True).over(w3).alias("first_known_account_type"),
        F.row_number().over(w4).alias("event_seq_2"),
    )
)

enriched_transactions = (
    scored
    .filter(F.col("event_seq_2").between(4, 8))
    .select(
        "id",
        "account_type",
        "event_seq",
        "last_known_account_type",
        "event_seq_2",
        F.avg("event_seq_2").over(w5).alias("event_seq_2_avg_2"),
        F.first("event_seq_2", ignorenulls=True).over(w6).alias("first_known_event_seq_2"),
    )
)

scored_transactions = (
    enriched_transactions
    .withColumn("event_seq_bucket", F.ntile(10).over(w7))
    .withColumn("next_event_seq_2_avg_2", F.lead("event_seq_2_avg_2", 1).over(w8))
    .withColumn("account_type_sum_event_seq_2_avg_2", F.sum("event_seq_2_avg_2").over(w9))
)

result = (
    scored_transactions
    .select(
        "id",
        "event_seq",
        "event_seq_2",
        "first_known_event_seq_2",
        "event_seq_bucket",
    )
    .orderBy("id", "event_seq")
    .limit(10)
)
