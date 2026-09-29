from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_joined_on_month_statu = Window.partitionBy(F.date_trunc("month", "holder_joined_on"), "status")
w_end_date_year_holder_segment = Window.partitionBy(F.year("end_date"), "holder_segment")
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_premium = Window.orderBy(F.col("premium").desc(), "id")
w_product_group_by_holder_join = Window.partitionBy("product_group").orderBy(F.col("holder_joined_on").desc(), "id").rowsBetween(-6, Window.currentRow)
w_product_group_holder_id_by_e = Window.partitionBy("product_group", "holder_id").orderBy(F.col("end_date").desc(), "id")
w_status_product = Window.partitionBy("status", "product")
w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_premium_full = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_full = Window.orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_last_start_date_ye = Window.partitionBy("holder_id", F.year("last_start_date")).orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_last_start_date_ye_2 = Window.partitionBy("holder_id", F.year("last_start_date")).orderBy("start_date")
w_holder_id = Window.partitionBy("holder_id")
w_holder_segment_by_premium_vs = Window.partitionBy("holder_segment").orderBy("premium_vs_status_product_avg")
w_all_by_start_date_running_2 = Window.orderBy(F.col("start_date").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = (
    df_policies
    .filter((F.col("status") != "ACTIVE") & (F.col("premium") >= 2096.6))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.concat(F.col("status"), F.lit("#"), F.col("holder_id").cast("string")).alias("status_ref"),
        F.when(F.col("product").isin("AUTO", "HOME"), "group_a").when(F.col("product") == "LIFE", "group_b").otherwise("other").alias("product_group"),
    )
)

joined_holder = (
    prepared.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        "s.end_date",
        "s.status",
        "s.status_ref",
        "s.product_group",
        F.col("p.segment").alias("holder_segment"),
        F.col("p.joined_on").alias("holder_joined_on"),
        "p.holder_name",
    )
)

ranked = (
    joined_holder
    .select(
        "*",
        F.size(F.collect_set("product_group").over(w_holder_joined_on_month_statu)).alias("distinct_product_group_in_holder_joined_on_month"),
        F.sum("premium").over(w_end_date_year_holder_segment).alias("end_date_year_holder_segment_sum_premium"),
        F.min("premium").over(w_all_by_start_date_running).alias("min_premium_so_far"),
        F.round(F.percent_rank().over(w_all_by_premium), 3).alias("premium_percent_rank"),
        F.round(F.avg("premium").over(w_product_group_by_holder_join), 2).alias("premium_avg_7"),
        F.lead("premium").over(w_product_group_holder_id_by_e).alias("next_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_status_product), 2).alias("premium_vs_status_product_avg"),
        F.round(F.cume_dist().over(w_all_by_start_date), 3).alias("start_date_cume_dist"),
        F.row_number().over(w_all_by_premium).alias("seq_no"),
        F.last("start_date").over(w_all_by_premium_full).alias("last_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_all_by_premium)).alias("days_since_prev_start_date"),
        F.sum("premium").over(w_holder_joined_on_month_statu).alias("holder_joined_on_month_status_sum_premium"),
    )
)

sequenced = (
    ranked
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "product_group",
        "holder_segment",
        "holder_joined_on",
        "holder_name",
        "distinct_product_group_in_holder_joined_on_month",
        "end_date_year_holder_segment_sum_premium",
        "min_premium_so_far",
        "premium_vs_status_product_avg",
        "start_date_cume_dist",
        "seq_no",
        "last_start_date",
        "days_since_prev_start_date",
        F.last("seq_no").over(w_all_by_start_date_full).alias("last_seq_no"),
        F.nth_value("premium", 2).over(w_holder_id_last_start_date_ye).alias("second_premium"),
        F.rank().over(w_holder_id_last_start_date_ye_2).alias("start_date_rank"),
        F.count(F.lit(1)).over(w_holder_id).alias("rows_in_holder_id"),
        F.round(F.percent_rank().over(w_holder_segment_by_premium_vs), 3).alias("premium_vs_status_product_avg_percent_rank"),
        F.max("premium_avg_7").over(w_all_by_start_date_running_2).alias("max_premium_avg_7_so_far"),
        F.round(F.col("premium_percent_rank") - F.avg("premium_percent_rank").over(w_holder_id), 2).alias("premium_percent_rank_vs_holder_id_avg"),
    )
)

result = (
    sequenced
    .select(
        "id",
        "premium",
        "start_date",
        "end_date",
        "status",
        "holder_segment",
        "holder_name",
        "distinct_product_group_in_holder_joined_on_month",
        "end_date_year_holder_segment_sum_premium",
        "premium_vs_status_product_avg",
        "last_start_date",
        "last_seq_no",
        "second_premium",
        "premium_vs_status_product_avg_percent_rank",
        "premium_percent_rank_vs_holder_id_avg",
    )
)
