from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("merchant").orderBy(F.col("txn_time_day").asc_nulls_last(), "id")
w2 = Window.partitionBy("merchant", "txn_type").orderBy(F.col("txn_time_day").desc(), "id")
w3 = Window.partitionBy("txn_type").orderBy("txn_time", "id")
w4 = Window.orderBy(F.col("txn_time").desc(), "id")
w5 = Window.partitionBy("txn_type").orderBy(F.col("row_num").desc(), "id")

cleaned = (
    df_transactions
    .filter((F.col("txn_time") >= F.to_timestamp(F.lit("2023-07-01 01:00:00"))) & F.col("account_id").isin([7, 21, 31, 39]))
    .select(
        "id",
        "account_id",
        "txn_type",
        "txn_time",
        "merchant",
        "is_flagged",
        F.col("txn_time").cast("date").alias("txn_time_day"),
    )
)

ranked = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

deduped = (
    ranked
    .withColumn("row_num_2", F.row_number().over(w2))
    .filter(F.col("row_num_2") == 1)
    .select("id", "txn_type", "txn_time", "txn_time_day", "row_num")
)

enriched = (
    deduped
    .select(
        "id",
        "txn_type",
        "row_num",
        F.row_number().over(w3).alias("row_seq"),
        F.row_number().over(w4).alias("event_seq"),
        F.row_number().over(w5).alias("event_seq_2"),
    )
)

cleaned_transactions = (
    enriched
    .filter(~F.col("txn_type").isin(["FEE", "DEBIT"]))
    .withColumn("event_seq_2_plus_row_num", (F.col("event_seq_2") + F.col("row_num")))
    .withColumn("greatest_row_num_event_seq", F.greatest("row_num", "event_seq"))
)

cleaned_transactions_2 = (
    cleaned_transactions
    .filter(F.col("row_num") >= 1)
    .select("*", F.round("row_num", 2).alias("row_num_rounded"))
)

base = (
    cleaned_transactions_2
    .filter(F.col("row_seq").between(6, 8))
    .select(
        "id",
        "row_num",
        "event_seq_2_plus_row_num",
        "row_num_rounded",
        F.col("event_seq_2").cast("double").alias("event_seq_2_str"),
        F.col("event_seq_2_plus_row_num").cast("int").alias("event_seq_2_plus_row_num_int"),
    )
)

result = (
    base
    .select(
        "id",
        "row_num",
        "row_num_rounded",
        "event_seq_2_str",
        "event_seq_2_plus_row_num_int",
    )
    .orderBy(F.col("row_num_rounded").desc())
)
