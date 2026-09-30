from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_status_len_moving_3 = Window.orderBy(F.col("status_len").asc_nulls_last(), "id").rowsBetween(-3, Window.currentRow)
w_status_holder_id = Window.partitionBy("status", "holder_id")
w_premium_tier_by_premium_full = Window.partitionBy("premium_tier").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_premium_tier_status_by_statu = Window.partitionBy("premium_tier", "status").orderBy("status_len", "id")
w_all_by_status_len = Window.orderBy(F.col("status_len").asc_nulls_last(), "id")
w_premium_tier_by_premium = Window.partitionBy("premium_tier").orderBy("premium", "id")
w_product_status = Window.partitionBy("product", "status")
w_holder_id_status_by_premium = Window.partitionBy("holder_id", "status").orderBy(F.col("premium").desc())
w_premium_tier_status_by_statu_2 = Window.partitionBy("premium_tier", "status").orderBy("status_len", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_status_len_quintile_seq_no_b = Window.partitionBy("status_len_quintile", "seq_no").orderBy("prev3_premium")
w_all_by_last_premium = Window.orderBy("last_premium", "id")
w_all_by_last_premium_full = Window.orderBy("last_premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_policys = (
    df_policies
    .filter((F.col("end_date") >= F.lit("2025-12-05").cast("date")) & (F.col("premium") >= 2033.8))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.length(F.trim(F.col("status"))).alias("status_len"),
        F.when(F.col("premium") >= 4417.0, "A").when(F.col("premium") >= 2096.6, "B").otherwise("C").alias("premium_tier"),
    )
)

with_trends = (
    cleaned_policys
    .select(
        "id",
        "premium",
        "end_date",
        "status",
        "premium_tier",
        F.round(F.avg("premium").over(w_all_by_status_len_moving_3), 2).alias("premium_avg_4"),
        F.size(F.collect_set("premium_tier").over(w_status_holder_id)).alias("distinct_premium_tier_in_status_holder_id"),
        F.nth_value("status_len", 2).over(w_premium_tier_by_premium_full).alias("second_status_len"),
        F.round(F.percent_rank().over(w_premium_tier_status_by_statu), 3).alias("status_len_percent_rank"),
        F.ntile(5).over(w_all_by_status_len).alias("status_len_quintile"),
        F.last("premium").over(w_premium_tier_by_premium_full).alias("last_premium"),
        F.lag("premium", 3).over(w_all_by_status_len).alias("prev3_premium"),
        F.count(F.lit(1)).over(w_status_holder_id).alias("rows_in_status_holder_id"),
        F.row_number().over(w_premium_tier_by_premium).alias("seq_no"),
        F.round(F.avg("premium").over(w_product_status), 2).alias("product_status_avg_premium"),
        F.round(F.cume_dist().over(w_holder_id_status_by_premium), 3).alias("premium_cume_dist"),
        F.first("status_len").over(w_premium_tier_status_by_statu_2).alias("first_status_len"),
    )
)

enriched = (
    with_trends
    .select(
        "id",
        "premium",
        "end_date",
        "second_status_len",
        "status_len_percent_rank",
        "status_len_quintile",
        "last_premium",
        "prev3_premium",
        "seq_no",
        "premium_cume_dist",
        "first_status_len",
        F.round(F.col("premium_cume_dist") / F.nullif(F.col("premium_avg_4"), F.lit(0)), 3).alias("premium_cume_dist_per_premium_avg_4"),
        (F.col("second_status_len") - F.col("first_status_len")).alias("second_status_len_minus_first_status_len"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "premium",
        "end_date",
        "status_len_quintile",
        "prev3_premium",
        "seq_no",
        "premium_cume_dist",
        "first_status_len",
        "premium_cume_dist_per_premium_avg_4",
        "second_status_len_minus_first_status_len",
        F.dense_rank().over(w_status_len_quintile_seq_no_b).alias("prev3_premium_dense_rank"),
        F.dense_rank().over(w_all_by_last_premium).alias("last_premium_dense_rank"),
        F.nth_value("prev3_premium", 2).over(w_all_by_last_premium_full).alias("second_prev3_premium"),
    )
)

result = (
    metrics
    .filter(F.col("premium_cume_dist_per_premium_avg_4") >= 4.5)
    .select(
        "id",
        "premium",
        "end_date",
        "status_len_quintile",
        "prev3_premium",
        "premium_cume_dist",
        "first_status_len",
        "premium_cume_dist_per_premium_avg_4",
        "second_status_len_minus_first_status_len",
        "prev3_premium_dense_rank",
        F.least(F.col("status_len_quintile"), F.col("premium")).alias("least_status_len_quintile_premium"),
        (F.col("premium_cume_dist") + F.col("seq_no")).alias("premium_cume_dist_plus_seq_no"),
    )
    .orderBy(F.col("premium_cume_dist").desc(), F.col("premium").desc(), "id")
    .limit(10)
)
