from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date")
w2 = Window.partitionBy("status", "account_type").orderBy(F.col("opened_date").desc(), "id")
w3 = Window.partitionBy("status").orderBy("opened_date", "id")
w4 = Window.partitionBy("account_type", "status").orderBy(F.col("opened_date").desc(), "id")
w5 = Window.partitionBy("account_type").orderBy("opened_date", "id")
w6 = Window.orderBy(F.col("opened_date").desc(), "id")

filtered = (
    df_accounts
    .filter(F.col("opened_date") > F.lit("2023-09-26").cast("date"))
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.date_trunc("WEEK", "opened_date").alias("opened_date_period"),
    )
)

kept = (
    filtered
    .alias("s")
    .join(df_transactions.alias("c"), (F.col("c.account_id") == F.col("s.id")) & (F.col("c.merchant").isNotNull()), "left_anti")
)

deduped = (
    kept
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "account_type", "opened_date", "status")
)

metrics = (
    deduped
    .select(
        "id",
        "account_type",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
        F.row_number().over(w4).alias("row_seq"),
        F.row_number().over(w5).alias("row_seq_2"),
        F.row_number().over(w6).alias("row_seq_3"),
    )
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_transactions.alias("x"), (F.col("x.account_id") == F.col("s.id")) & (~F.col("x.is_flagged")), "left_anti")
)

result = (
    not_flagged
    .groupBy(
        F.when(F.col("row_seq_3") >= 9, "LARGE").otherwise("SMALL").alias("row_seq_3_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count_if(F.col("account_type") == "CREDIT").alias("credit_events"),
        F.round(F.sum("row_seq_3") / F.nullif(F.count(F.when(F.col("account_type") == "SAVINGS", 1)), F.lit(0)), 2).alias("row_seq_3_per_savings"),
    )
    .orderBy(F.col("row_seq_3_per_savings").asc_nulls_first())
)
