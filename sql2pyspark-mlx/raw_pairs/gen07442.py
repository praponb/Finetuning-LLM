from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("method").orderBy("amount", "id")
w2 = Window.partitionBy("method").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("row_seq", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("running_sum_prev_amount").desc_nulls_last(), "id")
w5 = Window.orderBy("seq_no", "id")

base = (
    df_payments
    .filter((F.col("paid_at") > F.lit("2023-08-31 23:00:00").cast("timestamp")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "method",
        "amount",
        "status",
        F.when(F.col("method").isin(["card", "paypal"]), "Priority").otherwise("Other").alias("method_group"),
    )
)

with_windows = (
    base
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("row_seq", F.row_number().over(w))
)

scored = (
    with_windows
    .select(
        "id",
        "order_id",
        "method_group",
        "prev_amount",
        F.sum("prev_amount").over(w2).alias("running_sum_prev_amount"),
        F.min("prev_amount").over(w2).alias("running_min_prev_amount"),
        F.count("prev_amount").over(w2).alias("running_count_prev_amount"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w4))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "order_id",
        "method_group",
        "prev_amount",
        "running_sum_prev_amount",
        "running_min_prev_amount",
        "running_count_prev_amount",
        "seq_no",
    )
)

cleaned = (
    latest
    .filter(F.col("prev_amount").between(630, 1160))
    .select(
        "id",
        "running_sum_prev_amount",
        "running_min_prev_amount",
        "running_count_prev_amount",
        "seq_no",
        F.greatest("running_min_prev_amount", "prev_amount").alias("greatest_running_min_prev_amount_prev_amount"),
    )
)

with_windows_payments = (
    cleaned
    .select(
        "*",
        F.lead("greatest_running_min_prev_amount_prev_amount").over(w5).alias("next_greatest_running_min_prev_amount_prev_amount"),
        F.lag("seq_no").over(w5).alias("prev_seq_no"),
        F.lead("seq_no").over(w5).alias("next_seq_no"),
        F.row_number().over(w5).alias("row_seq"),
    )
)

result = (
    with_windows_payments
    .withColumn("running_sum_prev_amount_tier", F.when(F.col("running_sum_prev_amount") >= 7100, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["prev_seq_no", "running_sum_prev_amount_tier"], ["prev_seq_no"]],
        "prev_seq_no",
        "running_sum_prev_amount_tier",
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_sum_prev_amount", [0.25, 0.5, 0.75]).alias("running_sum_prev_amount_quartiles"),
    )
    .filter(F.col("distinct_ids") >= 1)
)
