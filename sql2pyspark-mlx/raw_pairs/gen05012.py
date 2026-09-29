from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("merchant").orderBy("amount", "id")
w2 = Window.partitionBy("merchant")
w3 = Window.orderBy("amount_whole", "id")
w_hist = Window.orderBy(F.col("next_amount_whole").asc_nulls_first())
w5 = Window.orderBy("next_amount_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy(F.to_date("txn_time")).orderBy(F.col("txn_time").desc(), "id")
w8 = Window.partitionBy(F.to_date("txn_time")).orderBy("prev_amount_whole", "id").rowsBetween(-2, Window.currentRow)
w9 = Window.partitionBy(F.year("txn_time")).orderBy("seq_no", "id")

valid = (
    df_transactions
    .filter((F.col("txn_time") >= F.lit("2023-06-03").cast("date")) & F.col("merchant").isNotNull())
    .withColumn("amount_whole", F.floor("amount"))
    .withColumn("merchant_lower", F.lower("merchant"))
)

enriched = (
    valid
    .filter(F.hour("txn_time").between(8, 14))
    .select(
        "id",
        "amount",
        "txn_time",
        "amount_whole",
        "merchant_lower",
        F.lag("amount").over(w_recent).alias("prev_amount"),
        F.lead("amount_whole").over(w_recent).alias("next_amount_whole"),
        F.lag("amount_whole").over(w_recent).alias("prev_amount_whole"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.max("amount").over(w2).alias("merchant_max_amount"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "txn_time",
        "merchant_lower",
        "next_amount_whole",
        "prev_amount_whole",
        "seq_no",
        "merchant_max_amount",
        F.ntile(4).over(w3).alias("amount_whole_bucket"),
        F.rank().over(w_hist).alias("next_amount_whole_rank"),
        F.first("prev_amount", ignorenulls=True).over(w5).alias("first_known_prev_amount"),
        F.last("merchant_max_amount", ignorenulls=True).over(w_hist_2).alias("last_known_merchant_max_amount"),
        F.sum("amount").over(w_hist_2).alias("running_amount"),
    )
)

enriched_transactions = (
    metrics
    .select(
        "id",
        "merchant_lower",
        "next_amount_whole",
        "prev_amount_whole",
        "seq_no",
        "merchant_max_amount",
        "last_known_merchant_max_amount",
        "running_amount",
        F.row_number().over(w7).alias("seq_no_2"),
        F.avg("merchant_max_amount").over(w8).alias("merchant_max_amount_avg_2"),
        F.row_number().over(w9).alias("event_seq"),
    )
)

result = (
    enriched_transactions
    .groupBy("seq_no")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.avg("prev_amount_whole"), 2).alias("avg_prev_amount_whole"),
        F.min("seq_no").alias("min_seq_no"),
        F.percentile_approx("next_amount_whole", 0.95).alias("next_amount_whole_p95"),
    )
    .orderBy("min_seq_no")
)
