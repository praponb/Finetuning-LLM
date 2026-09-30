from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_status_end_date_month_by_end = Window.partitionBy("status", F.date_trunc("month", "end_date")).orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_end_date = Window.orderBy(F.col("end_date").asc_nulls_last(), "id")
w_product = Window.partitionBy("product")
w_status_end_date_month_by_end_2 = Window.partitionBy("status", F.date_trunc("month", "end_date")).orderBy(F.col("end_date").desc())
w_end_date_month_by_premium = Window.partitionBy(F.month("end_date")).orderBy(F.col("premium").desc(), "id")
w_all_by_end_date_full = Window.orderBy(F.col("end_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_second_end_date_ye = Window.partitionBy("holder_id", F.year("second_end_date")).orderBy("next2_end_date", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_second_end_date_ye_2 = Window.partitionBy("holder_id", F.year("second_end_date")).orderBy("next2_end_date", "id")
w_holder_id_second_end_date_ye_3 = Window.partitionBy("holder_id", F.year("second_end_date")).orderBy("next2_end_date")

base_policys = (
    df_policies
    .filter((F.col("status").isin("ACTIVE", "EXPIRED")) & (F.col("start_date").isNotNull()))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        "status",
        F.when(F.col("premium") >= 3465.4, "A").when(F.col("premium") >= 2096.6, "B").otherwise("C").alias("premium_tier"),
    )
)

windowed = (
    base_policys
    .select(
        "*",
        F.max("premium").over(w_status_end_date_month_by_end).alias("max_premium_so_far"),
        F.round(F.cume_dist().over(w_all_by_end_date), 3).alias("end_date_cume_dist"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product), F.lit(0)), 4).alias("premium_share_of_product"),
        F.size(F.collect_set("status").over(w_product)).alias("distinct_status_in_product"),
        F.round(F.cume_dist().over(w_status_end_date_month_by_end_2), 3).alias("end_date_cume_dist_2"),
        F.row_number().over(w_end_date_month_by_premium).alias("rn"),
        F.nth_value("end_date", 2).over(w_all_by_end_date_full).alias("second_end_date"),
        F.row_number().over(w_all_by_end_date).alias("end_date_seq"),
        F.lead("end_date", 2).over(w_all_by_end_date).alias("next2_end_date"),
    )
)

top_per_group = (
    windowed
    .filter(F.col("rn") == 1)
    .drop("rn")
)

windowed_2 = (
    top_per_group
    .select(
        "*",
        F.last("distinct_status_in_product").over(w_holder_id_second_end_date_ye).alias("last_distinct_status_in_product"),
        F.lag("end_date_seq", 2).over(w_holder_id_second_end_date_ye_2).alias("prev2_end_date_seq"),
        F.dense_rank().over(w_holder_id_second_end_date_ye_2).alias("next2_end_date_dense_rank"),
        F.dense_rank().over(w_holder_id_second_end_date_ye_3).alias("next2_end_date_dense_rank_3"),
    )
)

result = (
    windowed_2
    .select(
        "id",
        "holder_id",
        "premium",
        "end_date",
        "status",
        "premium_tier",
        "max_premium_so_far",
        "end_date_cume_dist",
        "premium_share_of_product",
        "distinct_status_in_product",
        "end_date_cume_dist_2",
        "end_date_seq",
        "next2_end_date",
        "next2_end_date_dense_rank_3",
        F.abs(F.col("prev2_end_date_seq") - 7).alias("prev2_end_date_seq_distance"),
    )
    .orderBy(F.col("max_premium_so_far").desc(), "premium_share_of_product")
)
