from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_valid_to_year_tariff = Window.partitionBy(F.year("valid_to"), "tariff")
w_tariff_valid_to_month_by_val = Window.partitionBy("tariff", F.month("valid_to")).orderBy("valid_from", "id")
w_tariff_valid_to_month_by_val_2 = Window.partitionBy("tariff", F.month("valid_to")).orderBy("valid_from", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_tariff_by_rate_full = Window.partitionBy("tariff").orderBy("rate", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_valid_to_month_by_val_3 = Window.partitionBy("tariff", F.month("valid_to")).orderBy("valid_from")
w_tariff_by_rate = Window.partitionBy("tariff").orderBy("rate", "id")
w_valid_from_month_tariff_by_r = Window.partitionBy(F.month("valid_from"), "tariff").orderBy(F.col("rate").desc_nulls_first(), "id")
w_valid_from_month_tariff_by_r_2 = Window.partitionBy(F.month("valid_from"), "tariff").orderBy(F.col("rate").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_tariff_valid_to_month_by_val_4 = Window.partitionBy("tariff", F.month("valid_to")).orderBy("valid_from", "id").rowsBetween(-3, -1)

valid = (
    df_tariff_windows
    .filter((F.col("tariff") == "TOU") & (F.col("valid_from") >= F.lit("2025-01-23 19:00:00").cast("timestamp")))
    .select(
        "id",
        "tariff",
        "valid_from",
        "valid_to",
        "rate",
        F.date_format(F.col("valid_to"), "yyyy-MM-dd HH:00").alias("valid_to_label"),
    )
)

with_trends = (
    valid
    .select(
        "*",
        F.sum("rate").over(w_valid_to_year_tariff).alias("valid_to_year_tariff_sum_rate"),
        F.lag("rate").over(w_tariff_valid_to_month_by_val).alias("prev_rate"),
        F.count(F.lit(1)).over(w_tariff_valid_to_month_by_val_2).alias("running_count"),
        F.round(F.col("rate") - F.avg("rate").over(w_valid_to_year_tariff), 2).alias("rate_vs_valid_to_year_tariff_avg"),
        F.round(F.col("rate") / F.nullif(F.sum("rate").over(w_valid_to_year_tariff), F.lit(0)), 4).alias("rate_share_of_valid_to_year_tariff"),
        F.nth_value("valid_to", 2).over(w_tariff_by_rate_full).alias("second_valid_to"),
        F.round(F.cume_dist().over(w_tariff_valid_to_month_by_val_3), 3).alias("valid_from_cume_dist"),
        F.row_number().over(w_tariff_by_rate).alias("seq_no"),
        F.row_number().over(w_valid_from_month_tariff_by_r).alias("rate_seq"),
        F.first("valid_to").over(w_valid_from_month_tariff_by_r_2).alias("first_valid_to"),
        F.ntile(10).over(w_tariff_by_rate).alias("rate_decile"),
        F.round(F.avg("rate").over(w_tariff_valid_to_month_by_val_4), 2).alias("rate_avg_3_prior"),
        F.lead("valid_to").over(w_valid_from_month_tariff_by_r).alias("next_valid_to"),
        F.lead("valid_to", 2).over(w_tariff_by_rate).alias("next2_valid_to"),
    )
)

result = (
    with_trends
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        "valid_to_label",
        "prev_rate",
        "running_count",
        "rate_vs_valid_to_year_tariff_avg",
        "rate_share_of_valid_to_year_tariff",
        "second_valid_to",
        "valid_from_cume_dist",
        "seq_no",
        "rate_seq",
        "rate_decile",
        "rate_avg_3_prior",
        "next_valid_to",
        "next2_valid_to",
    )
    .orderBy(
        F.col("rate_vs_valid_to_year_tariff_avg").asc(),
        F.col("prev_rate").desc(),
    )
)
