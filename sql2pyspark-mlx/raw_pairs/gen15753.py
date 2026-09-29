from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("status").orderBy("paid_at", "id").rowsBetween(-3, Window.currentRow)
w_hist = Window.partitionBy("order_id").orderBy("paid_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("status").orderBy("last4_count_amount", "id")
w4 = Window.partitionBy("method").orderBy(F.col("cumulative_amount").desc(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("last4_min_amount").desc(), "id")
w_running = Window.partitionBy("method").orderBy("cumulative_amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status", "method").orderBy(F.col("last4_count_amount").desc(), "id")

with_windows = (
    df_payments
    .select(
        "id",
        "method",
        "amount",
        "status",
        F.count("amount").over(w_grp).alias("last4_count_amount"),
        F.avg("amount").over(w_grp).alias("last4_avg_amount"),
        F.sum("amount").over(w_grp).alias("last4_sum_amount"),
        F.min("amount").over(w_grp).alias("last4_min_amount"),
        F.sum("amount").over(w_hist).alias("cumulative_amount"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.row_number().over(w3).alias("seq_no"),
        F.row_number().over(w4).alias("row_seq"),
    )
)

ranked = (
    scored
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

scored_payments = (
    ranked
    .withColumn("running_max_last4_min_amount", F.max("last4_min_amount").over(w_running))
    .withColumn("running_sum_last4_min_amount", F.sum("last4_min_amount").over(w_running))
    .withColumn("running_min_last4_min_amount", F.min("last4_min_amount").over(w_running))
)

ranked_payments = (
    scored_payments
    .withColumn("latest_rank", F.row_number().over(w7))
    .filter(F.col("latest_rank") == 1)
)

kept_keys = (
    ranked_payments
    .select("id", "method")
    .subtract(
        df_payments
        .filter(F.col("method") == "card")
        .select("id", "method")
    )
)

reconciled = (
    ranked_payments
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.method") == F.col("k.method")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "method",
        "amount",
        "status",
        "last4_sum_amount",
        "last4_min_amount",
        "cumulative_amount",
        "seq_no",
        "latest_rank",
        F.when(F.col("status").rlike("^(FAILED|SUCCESS)$"), "PRIORITY").otherwise("CORE").alias("status_zone"),
    )
    .orderBy(F.col("latest_rank").asc_nulls_first())
)
