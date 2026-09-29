from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy(F.date_trunc("MONTH", "paid_at"), "method").orderBy(F.col("paid_at").desc(), "id")
w2 = Window.partitionBy("status").orderBy("amount", "id").rowsBetween(-3, -1)
w3 = Window.orderBy(F.col("amount_avg_3_prior_scaled_filled_filled").desc_nulls_last())
w4 = Window.orderBy("amount_avg_3_prior_scaled", "id")
w5 = Window.orderBy(F.col("amount_avg_3_prior_filled").desc(), "id")
w_grp = Window.orderBy("amount_avg_3_prior_scaled_filled_filled_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("amount_avg_3_prior_scaled_filled_filled_rank", "id").rowsBetween(-2, -1)

metrics = (
    df_payments
    .select(
        "id",
        "paid_at",
        F.row_number().over(w_hist).alias("seq_no"),
        F.avg("amount").over(w2).alias("amount_avg_3_prior"),
    )
)

valid = (
    metrics
    .filter((F.col("paid_at") > F.lit("2023-11-14 16:00:00").cast("timestamp")) & F.hour("paid_at").between(12, 17))
    .select("*", F.to_date("paid_at").alias("paid_at_date"))
)

valid_payments = (
    valid
    .filter(F.to_date("paid_at") < "2024-02-01")
    .select(
        "id",
        "paid_at",
        "amount_avg_3_prior",
        "paid_at_date",
        F.round((F.col("amount_avg_3_prior") * 0.9), 2).alias("amount_avg_3_prior_scaled"),
        (F.col("seq_no") - F.col("amount_avg_3_prior")).alias("seq_no_minus_amount_avg_3_prior"),
    )
)

valid_payments_2 = (
    valid_payments
    .filter(F.col("paid_at") > F.lit("2023-10-16").cast("date"))
    .select(
        "*",
        F.add_months("paid_at_date", 3).alias("paid_at_date_plus_months"),
        F.coalesce("amount_avg_3_prior_scaled", F.lit(0)).alias("amount_avg_3_prior_scaled_filled"),
    )
)

cleaned = (
    valid_payments_2
    .filter(F.col("paid_at") > F.lit("2023-10-29 22:00:00").cast("timestamp"))
    .select(
        "id",
        "paid_at_date",
        "amount_avg_3_prior_scaled",
        "paid_at_date_plus_months",
        F.coalesce("amount_avg_3_prior", F.lit(0)).alias("amount_avg_3_prior_filled"),
        F.coalesce("amount_avg_3_prior_scaled_filled", F.lit(0)).alias("amount_avg_3_prior_scaled_filled_filled"),
    )
)

scored = (
    cleaned
    .filter(F.col("amount_avg_3_prior_filled") >= 34)
    .select(
        "id",
        "amount_avg_3_prior_scaled",
        "amount_avg_3_prior_scaled_filled_filled",
        F.rank().over(w3).alias("amount_avg_3_prior_scaled_filled_filled_rank"),
        F.lead("amount_avg_3_prior_scaled", 1).over(w4).alias("next_amount_avg_3_prior_scaled"),
        F.ntile(10).over(w5).alias("amount_avg_3_prior_filled_bucket"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.min("amount_avg_3_prior_filled_bucket").over(w_grp).alias("running_min_amount_avg_3_prior_filled_bucket"),
        F.sum("amount_avg_3_prior_filled_bucket").over(w_grp).alias("running_sum_amount_avg_3_prior_filled_bucket"),
        F.count("amount_avg_3_prior_filled_bucket").over(w_grp).alias("running_count_amount_avg_3_prior_filled_bucket"),
        F.avg("amount_avg_3_prior_scaled_filled_filled_rank").over(w7).alias("amount_avg_3_prior_scaled_filled_filled_rank_avg_2_prior"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "amount_avg_3_prior_scaled",
        "amount_avg_3_prior_filled_bucket",
        "running_min_amount_avg_3_prior_filled_bucket",
        "running_sum_amount_avg_3_prior_filled_bucket",
        "running_count_amount_avg_3_prior_filled_bucket",
    )
    .orderBy(F.col("amount_avg_3_prior_scaled").desc_nulls_first())
)
