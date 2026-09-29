from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("paid_at").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.orderBy("amount", "id")
w3 = Window.partitionBy("order_id").orderBy("amount_max_30d", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("paid_at").desc(), "id")
w5 = Window.orderBy("prev_amount_max_30d_minus_next_amount_max_30d", "id")
w_ordered = Window.orderBy("row_seq")
w7 = Window.orderBy(F.col("prev_amount_max_30d_minus_next_amount_max_30d").desc())
w_grp = Window.orderBy(F.col("row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d").desc())

enriched = (
    df_payments
    .filter(F.col("order_id").isin([1, 7, 39]))
    .select(
        "id",
        "order_id",
        "paid_at",
        F.max("amount").over(w).alias("amount_max_30d"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "paid_at",
        "row_seq",
        F.lead("row_seq").over(w3).alias("next_row_seq"),
        F.lag("amount_max_30d").over(w3).alias("prev_amount_max_30d"),
        F.lead("amount_max_30d").over(w3).alias("next_amount_max_30d"),
        F.row_number().over(w3).alias("row_seq_2"),
        F.row_number().over(w4).alias("row_seq_3"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("paid_at") >= "2023-05-01")
    .select(
        "id",
        "row_seq",
        "row_seq_2",
        "row_seq_3",
        (F.col("row_seq_2") + F.col("next_amount_max_30d")).alias("row_seq_2_plus_next_amount_max_30d"),
        (F.col("prev_amount_max_30d") - F.col("next_amount_max_30d")).alias("prev_amount_max_30d_minus_next_amount_max_30d"),
    )
)

valid = (
    cleaned
    .filter(F.col("prev_amount_max_30d_minus_next_amount_max_30d") > 72)
    .select(
        "*",
        F.coalesce("prev_amount_max_30d_minus_next_amount_max_30d", F.lit(0)).alias("prev_amount_max_30d_minus_next_amount_max_30d_filled"),
        (F.col("row_seq_2_plus_next_amount_max_30d") - F.col("prev_amount_max_30d_minus_next_amount_max_30d")).alias("row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d"),
    )
)

with_windows_payments = (
    valid
    .select(
        "id",
        "prev_amount_max_30d_minus_next_amount_max_30d",
        "prev_amount_max_30d_minus_next_amount_max_30d_filled",
        "row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d",
        F.row_number().over(w5).alias("row_seq_4"),
        F.rank().over(w_ordered).alias("row_seq_rank"),
        F.cume_dist().over(w7).alias("prev_amount_max_30d_minus_next_amount_max_30d_cume_dist"),
        F.dense_rank().over(w_grp).alias("row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d_dense_rank"),
    )
)

result = (
    with_windows_payments
    .select(
        "id",
        "row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d",
        "row_seq_4",
        "prev_amount_max_30d_minus_next_amount_max_30d_cume_dist",
        "row_seq_2_plus_next_amount_max_30d_minus_prev_amount_max_30d_minus_next_amount_max_30d_dense_rank",
    )
    .orderBy(
        F.col("prev_amount_max_30d_minus_next_amount_max_30d_cume_dist").asc_nulls_last(),
        "row_seq_4",
    )
)
