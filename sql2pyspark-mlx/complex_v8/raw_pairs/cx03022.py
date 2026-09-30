from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_tariff = Window.partitionBy("tariff")
w_all_by_rate = Window.orderBy(F.col("rate").desc(), "id")
w_seq_no_first_valid_to_to_dat = Window.partitionBy("seq_no", F.to_date("first_valid_to"))
w_distinct_ids_seq_no_tier = Window.partitionBy("distinct_ids", "seq_no_tier")
w_all_by_first_valid_to_full = Window.orderBy(F.col("first_valid_to").desc(), "seq_no").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

prepared = df_tariff_windows.filter((F.col("rate") > 3.0) & (F.col("tariff") == "FLAT"))

with_windows = (
    prepared
    .select(
        "id",
        "valid_from",
        "valid_to",
        "rate",
        F.round(F.col("rate") - F.avg("rate").over(w_tariff), 2).alias("rate_vs_tariff_avg"),
        F.row_number().over(w_all_by_rate).alias("seq_no"),
    )
)

grouped = (
    with_windows
    .groupBy("seq_no")
    .agg(
        F.min("valid_to").alias("first_valid_to"),
        F.max("valid_to").alias("last_valid_to"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

sequenced = (
    grouped
    .select(
        "seq_no",
        "first_valid_to",
        "last_valid_to",
        "distinct_ids",
        F.round(F.col("seq_no") / F.nullif(F.sum("seq_no").over(w_seq_no_first_valid_to_to_dat), F.lit(0)), 4).alias("seq_no_share_of_seq_no_first_valid_to_to_date"),
        F.sum("seq_no").over(w_seq_no_first_valid_to_to_dat).alias("seq_no_first_valid_to_to_date_sum_seq_no"),
    )
)

labeled = (
    sequenced
    .select(
        "*",
        F.when(F.col("seq_no") >= 9, "large").when(F.col("seq_no") >= 4, "medium").otherwise("small").alias("seq_no_tier"),
        (F.col("seq_no") - F.col("distinct_ids")).alias("seq_no_minus_distinct_ids"),
        F.round(F.col("seq_no") / F.nullif(F.col("distinct_ids"), F.lit(0)), 3).alias("seq_no_per_distinct_ids"),
    )
)

with_trends = (
    labeled
    .select(
        "seq_no",
        "seq_no_share_of_seq_no_first_valid_to_to_date",
        "seq_no_first_valid_to_to_date_sum_seq_no",
        "seq_no_minus_distinct_ids",
        F.min("distinct_ids").over(w_distinct_ids_seq_no_tier).alias("distinct_ids_seq_no_tier_min_distinct_ids"),
        F.last("last_valid_to").over(w_all_by_first_valid_to_full).alias("last_last_valid_to"),
        F.count(F.lit(1)).over(w_distinct_ids_seq_no_tier).alias("rows_in_distinct_ids_seq_no_tier"),
        F.round(F.col("seq_no") - F.avg("seq_no").over(w_distinct_ids_seq_no_tier), 2).alias("seq_no_vs_distinct_ids_seq_no_tier_avg"),
    )
)

derived = (
    with_trends
    .select(
        "seq_no",
        "seq_no_share_of_seq_no_first_valid_to_to_date",
        "seq_no_first_valid_to_to_date_sum_seq_no",
        "seq_no_minus_distinct_ids",
        "distinct_ids_seq_no_tier_min_distinct_ids",
        "seq_no_vs_distinct_ids_seq_no_tier_avg",
        F.when(F.col("seq_no") >= 6, "large").when(F.col("seq_no") >= 3, "medium").otherwise("small").alias("seq_no_tier"),
        (F.floor(F.col("seq_no") / 2) * 2).alias("seq_no_bucket"),
        F.abs(F.col("seq_no") - 7).alias("seq_no_distance"),
    )
)

result = (
    derived
    .select(
        "seq_no",
        "seq_no_share_of_seq_no_first_valid_to_to_date",
        "seq_no_first_valid_to_to_date_sum_seq_no",
        "distinct_ids_seq_no_tier_min_distinct_ids",
        "seq_no_vs_distinct_ids_seq_no_tier_avg",
        "seq_no_bucket",
        "seq_no_distance",
        F.length(F.trim(F.col("seq_no_tier"))).alias("seq_no_tier_len"),
    )
)
