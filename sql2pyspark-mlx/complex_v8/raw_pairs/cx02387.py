from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_month_status_or_unk = Window.partitionBy(F.date_trunc("month", "end_date"), "status_or_unknown").orderBy("premium", "id")
w_holder_id = Window.partitionBy("holder_id")
w_end_date_month_status_or_unk_2 = Window.partitionBy(F.date_trunc("month", "end_date"), "status_or_unknown").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_by_premium_moving_3 = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(-3, Window.currentRow)
w_status_by_premium_moving_6 = Window.partitionBy("status").orderBy(F.col("premium").desc(), "id").rowsBetween(-6, Window.currentRow)
w_status_or_unknown_by_premium = Window.partitionBy("status_or_unknown").orderBy(F.col("premium").desc_nulls_first())
w_status_or_unknown_by_premium_2 = Window.partitionBy("status_or_unknown").orderBy(F.col("premium").desc_nulls_first(), "id")

filtered_policys = (
    df_policies
    .filter(F.col("holder_id") >= 10)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.coalesce(F.col("status"), F.lit("UNKNOWN")).alias("status_or_unknown"),
        F.substring(F.trim(F.col("product")), 1, 2).alias("product_prefix"),
    )
)

with_windows = (
    filtered_policys
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status_or_unknown",
        "product_prefix",
        F.dense_rank().over(w_end_date_month_status_or_unk).alias("premium_dense_rank"),
        F.size(F.collect_set("status_or_unknown").over(w_holder_id)).alias("distinct_status_or_unknown_in_holder_id"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.round(F.avg("premium").over(w_holder_id), 2).alias("holder_id_avg_premium"),
        F.last("premium").over(w_end_date_month_status_or_unk_2).alias("last_premium"),
        F.max("premium").over(w_status_by_premium_moving_3).alias("premium_max_4"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.round(F.avg("premium").over(w_status_by_premium_moving_6), 2).alias("premium_avg_7"),
        F.round(F.cume_dist().over(w_status_or_unknown_by_premium), 3).alias("premium_cume_dist"),
        F.min("premium").over(w_holder_id).alias("holder_id_min_premium"),
        F.size(F.collect_set("status").over(w_holder_id)).alias("distinct_status_in_holder_id"),
        F.lead("premium").over(w_status_or_unknown_by_premium_2).alias("next_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id), F.lit(0)), 4).alias("premium_share_of_holder_id"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.add_months(F.col("start_date"), 3).alias("start_date_plus_3m"),
        F.when(F.col("premium_max_4") >= 10560.0, "A").when(F.col("premium_max_4") >= 6120.0, "B").otherwise("C").alias("premium_max_4_tier"),
        (F.col("holder_id_avg_premium") - F.col("rows_in_holder_id")).alias("holder_id_avg_premium_minus_rows_in_holder_id"),
    )
)

result = (
    labeled
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "status_or_unknown",
        "product_prefix",
        "premium_dense_rank",
        "distinct_status_or_unknown_in_holder_id",
        "premium_vs_holder_id_avg",
        "holder_id_avg_premium",
        "last_premium",
        "premium_max_4",
        "rows_in_holder_id",
        "holder_id_min_premium",
        "distinct_status_in_holder_id",
        "next_premium",
        "holder_id_avg_premium_minus_rows_in_holder_id",
    )
    .orderBy(F.col("holder_id_avg_premium_minus_rows_in_holder_id").desc())
)
