from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id_by_holder_joined_o = Window.partitionBy("holder_id").orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id = Window.partitionBy("holder_id")
w_holder_id_by_holder_joined_o_2 = Window.partitionBy("holder_id").orderBy("holder_joined_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_segment_distinct_hold = Window.partitionBy("holder_segment", "distinct_holder_segment_in_holder_id").orderBy(F.col("distinct_holder_segment_in_holder_id").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_segment_distinct_hold_2 = Window.partitionBy("holder_segment", "distinct_holder_segment_in_holder_id").orderBy(F.col("distinct_holder_segment_in_holder_id").desc_nulls_first(), "id")
w_holder_segment_distinct_hold_3 = Window.partitionBy("holder_segment", "distinct_holder_segment_in_holder_id").orderBy(F.col("distinct_holder_segment_in_holder_id").desc_nulls_first())
w_product_distinct_holder_segm = Window.partitionBy("product", "distinct_holder_segment_in_holder_id")
w_product = Window.partitionBy("product")
w_holder_id_by_end_date_ties = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc_nulls_first())
w_holder_id_by_end_date = Window.partitionBy("holder_id").orderBy(F.col("end_date").desc_nulls_first(), "id")

valid_policys = (
    df_policies
    .filter(F.col("product") == "HOME")
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
    )
)

with_details = (
    valid_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.*",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
    )
)

windowed = (
    with_details
    .select(
        "*",
        F.nth_value("end_date", 2).over(w_holder_id_by_holder_joined_o).alias("second_end_date"),
        F.sum("premium").over(w_holder_id).alias("holder_id_sum_premium"),
        F.size(F.collect_set("holder_segment").over(w_holder_id)).alias("distinct_holder_segment_in_holder_id"),
        F.count(F.lit(1)).over(w_holder_id_by_holder_joined_o_2).alias("running_count"),
    )
)

windowed_2 = (
    windowed
    .select(
        "*",
        F.sum("premium").over(w_holder_segment_distinct_hold).alias("running_premium"),
        F.round(F.percent_rank().over(w_holder_segment_distinct_hold_2), 3).alias("distinct_holder_segment_in_holder_id_percent_ran"),
        F.dense_rank().over(w_holder_segment_distinct_hold_3).alias("distinct_holder_segment_in_holder_id_dense_rank"),
        F.size(F.collect_set("holder_segment").over(w_product_distinct_holder_segm)).alias("distinct_holder_segment_in_product_distinct_hold"),
    )
)

derived = (
    windowed_2
    .filter(F.col("holder_id_sum_premium") >= 5180.0)
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "holder_segment",
        "second_end_date",
        "holder_id_sum_premium",
        "running_count",
        "running_premium",
        "distinct_holder_segment_in_product_distinct_hold",
        F.when(F.col("holder_segment").isin("RETAIL", "SME"), "group_a").when(F.col("holder_segment") == "VIP", "group_b").otherwise("other").alias("holder_segment_group"),
        F.date_trunc("year", F.col("start_date")).alias("start_date_year"),
    )
)

metrics = (
    derived
    .select(
        "*",
        F.min("premium").over(w_product).alias("product_min_premium"),
        F.round(F.avg("premium").over(w_product), 2).alias("product_avg_premium"),
        F.dense_rank().over(w_holder_id_by_end_date_ties).alias("end_date_dense_rank"),
        F.count(F.lit(1)).over(w_product).alias("rows_in_product"),
        F.round(F.col("holder_id_sum_premium") / F.nullif(F.sum("holder_id_sum_premium").over(w_product), F.lit(0)), 4).alias("holder_id_sum_premium_share_of_product"),
        F.lag("start_date").over(w_holder_id_by_end_date).alias("prev_start_date"),
    )
)

result = (
    metrics
    .filter(F.col("second_end_date").between(F.lit("2025-09-27").cast("date"), F.lit("2027-03-20").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "holder_segment",
        "second_end_date",
        "running_count",
        "running_premium",
        "distinct_holder_segment_in_product_distinct_hold",
        "end_date_dense_rank",
        "rows_in_product",
        "prev_start_date",
    )
    .orderBy("second_end_date", F.col("rows_in_product").asc())
)
