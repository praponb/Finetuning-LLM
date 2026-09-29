from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_premium_running = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_segment_status_group = Window.partitionBy("holder_segment", "status_group").orderBy(F.col("premium").desc(), "id")
w_status = Window.partitionBy("status")
w_holder_id_by_premium = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id")
w_status_group = Window.partitionBy("status_group")
w_product_holder_segment = Window.partitionBy("product", "holder_segment")
w_holder_id_by_premium_full = Window.partitionBy("holder_id").orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium_full = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_status_holder_segment_by_pre = Window.partitionBy("status", "holder_segment").orderBy(F.col("premium").desc(), "id")
w_product_by_premium = Window.partitionBy("product").orderBy(F.col("premium").desc(), "id")
w_all_by_premium_2 = Window.orderBy("premium", "id")
w_premium_seq_premium_dense_ra = Window.partitionBy("premium_seq", "premium_dense_rank").orderBy("second_premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_premium_dense_rank_holder_se = Window.partitionBy("premium_dense_rank", "holder_segment").orderBy("second_premium_2", "id")

filtered_policys = (
    df_policies
    .filter(F.col("holder_id").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.when(F.col("status").isin("EXPIRED", "CANCELLED"), "group_a").when(F.col("status") == "LAPSED", "group_b").otherwise("other").alias("status_group"),
        F.when(F.col("premium") >= 2844.1, "HIGH").when(F.col("premium") >= 2033.8, "MEDIUM").otherwise("LOW").alias("premium_tier"),
    )
)

combined_details = (
    filtered_policys.alias("s")
    .join(df_policyholders.alias("p"), (F.col("p.id") == F.col("s.holder_id")) & (F.col("p.joined_on") < F.lit("2025-02-22").cast("date")), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.status",
        "s.status_group",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

windowed = (
    combined_details
    .select(
        "id",
        "premium",
        "status_group",
        "holder_segment",
        F.count(F.lit(1)).over(w_holder_id_by_premium_running).alias("running_count"),
        F.row_number().over(w_holder_segment_status_group).alias("premium_seq"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status), F.lit(0)), 4).alias("premium_share_of_status"),
        F.lag("premium", 3).over(w_holder_id_by_premium).alias("prev3_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_group), 2).alias("premium_vs_status_group_avg"),
        F.round(F.col("premium") - F.avg("premium").over(w_product_holder_segment), 2).alias("premium_vs_product_holder_segment_avg"),
        F.nth_value("premium", 2).over(w_holder_id_by_premium_full).alias("second_premium"),
        F.nth_value("premium", 2).over(w_all_by_premium_full).alias("second_premium_2"),
        F.row_number().over(w_all_by_premium).alias("rn"),
        F.lead("premium").over(w_status_holder_segment_by_pre).alias("next_premium"),
        F.size(F.collect_set("product").over(w_status)).alias("distinct_product_in_status"),
        (F.col("premium") - F.lag("premium").over(w_holder_segment_status_group)).alias("premium_change"),
        F.dense_rank().over(w_product_by_premium).alias("premium_dense_rank"),
        F.lead("premium", 2).over(w_all_by_premium_2).alias("next2_premium"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status_group), F.lit(0)), 4).alias("premium_share_of_status_group"),
    )
)

with_windows = (
    windowed
    .select(
        "*",
        F.sum("running_count").over(w_premium_seq_premium_dense_ra).alias("running_running_count"),
        F.ntile(4).over(w_premium_dense_rank_holder_se).alias("second_premium_2_quartile"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_status_group), F.lit(0)), 4).alias("premium_share_of_status_group_2"),
        F.round(F.col("premium_change") / F.nullif(F.sum("premium_change").over(w_status_group), F.lit(0)), 4).alias("premium_change_share_of_status_group"),
    )
)

result = (
    with_windows
    .filter(F.col("premium_vs_status_group_avg") < 3500.0)
    .select(
        "id",
        "status_group",
        "running_count",
        "premium_seq",
        "prev3_premium",
        "premium_vs_status_group_avg",
        "premium_vs_product_holder_segment_avg",
        "next_premium",
        "premium_change",
        "premium_dense_rank",
        "next2_premium",
        "premium_share_of_status_group",
        "running_running_count",
        "premium_share_of_status_group_2",
    )
    .orderBy("premium_dense_rank", F.col("running_count").desc())
)
