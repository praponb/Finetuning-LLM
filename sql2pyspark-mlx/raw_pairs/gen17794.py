from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "method").orderBy(F.col("paid_at").asc_nulls_last(), "id")
w2 = Window.partitionBy("status").orderBy(F.col("paid_at").desc_nulls_first(), "id")
w3 = Window.partitionBy("method").orderBy("amount", "id")
w_hist = Window.partitionBy("method").orderBy(F.col("amount").desc(), "id")
w5 = Window.partitionBy("method").orderBy("rank_in_group", "id")
w6 = Window.partitionBy("method").orderBy("rn", "id")

deduped = (
    df_payments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

latest = (
    deduped
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
    .select("id", "method", "amount", "rank_in_group")
)

scored = (
    latest
    .filter(F.col("method") == "bank_transfer")
    .select(
        "id",
        "method",
        "rank_in_group",
        F.ntile(10).over(w3).alias("amount_bucket"),
        F.row_number().over(w_hist).alias("row_seq"),
    )
)

ranked = (
    scored
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
)

scored_payments = (
    ranked
    .withColumn("prev_row_seq", F.lag("row_seq").over(w6))
    .withColumn("next_amount_bucket", F.lead("amount_bucket").over(w6))
    .withColumn("row_seq_2", F.row_number().over(w6))
)

result = (
    scored_payments
    .groupBy("method")
    .agg(
        F.percentile_approx("row_seq_2", [0.25, 0.5, 0.75]).alias("row_seq_2_quartiles"),
        F.percentile_approx("row_seq_2", 0.95).alias("row_seq_2_p95"),
    )
    .filter(F.col("row_seq_2_p95") < 1000000)
)
