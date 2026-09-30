from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_end_date_year_by_end_date = Window.partitionBy(F.year("end_date")).orderBy(F.col("end_date").desc_nulls_first(), "id")
w_end_date_month_product_group = Window.partitionBy(F.month("end_date"), "product_group").orderBy("premium", "id")
w_start_date_year_product_grou = Window.partitionBy(F.year("start_date"), "product_group").orderBy(F.col("end_date").desc(), "id").rowsBetween(-2, Window.currentRow)
w_all_by_end_date = Window.orderBy(F.col("end_date").desc(), "id")
w_start_date_year_product_grou_2 = Window.partitionBy(F.year("start_date"), "product_group").orderBy(F.col("end_date").desc(), "id")
w_product = Window.partitionBy("product")
w_end_date_year_by_end_date_ti = Window.partitionBy(F.year("end_date")).orderBy(F.col("end_date").desc_nulls_first())
w_end_date_month_product_group_2 = Window.partitionBy(F.month("end_date"), "product_group").orderBy("premium", "id").rowsBetween(-2, -1)
w_holder_id = Window.partitionBy("holder_id")
w_start_date_year_product_grou_3 = Window.partitionBy(F.year("start_date"), "product_group").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_start_date_year_product_grou_4 = Window.partitionBy(F.year("start_date"), "product_group").orderBy(F.col("end_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_product_rn = Window.partitionBy("product", "rn")
w_end_date_seq_by_prev3_premiu = Window.partitionBy("end_date_seq").orderBy("prev3_premium_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_end_date_rank_by_start_date = Window.partitionBy("end_date_rank").orderBy(F.col("start_date").desc(), "id")

scoped = (
    df_policies
    .filter(F.col("start_date").isNotNull())
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
        F.when(F.col("product").isin("HOME", "LIFE"), "group_a").when(F.col("product") == "TRAVEL", "group_b").otherwise("other").alias("product_group"),
        F.abs(F.col("premium") - 3465.4).alias("premium_distance"),
    )
)

with_windows = (
    scoped
    .select(
        "*",
        F.ntile(3).over(w_end_date_year_by_end_date).alias("end_date_tercile"),
        F.rank().over(w_end_date_month_product_group).alias("premium_rank"),
        F.round(F.avg("premium_distance").over(w_start_date_year_product_grou), 2).alias("premium_distance_avg_3"),
        F.lag("premium_distance", 3).over(w_all_by_end_date).alias("prev3_premium_distance"),
        F.row_number().over(w_start_date_year_product_grou_2).alias("rn"),
        F.size(F.collect_set("product_group").over(w_product)).alias("distinct_product_group_in_product"),
        F.row_number().over(w_all_by_end_date).alias("end_date_seq"),
        F.round(F.col("premium") - F.avg("premium").over(w_product), 2).alias("premium_vs_product_avg"),
        (F.col("premium_distance") - F.lag("premium_distance").over(w_end_date_month_product_group)).alias("premium_distance_change"),
        F.rank().over(w_end_date_year_by_end_date_ti).alias("end_date_rank"),
        F.max("premium_distance").over(w_end_date_month_product_group_2).alias("premium_distance_max_2_prior"),
        F.round(F.col("premium_distance") - F.avg("premium_distance").over(w_holder_id), 2).alias("premium_distance_vs_holder_id_avg"),
        F.nth_value("premium", 2).over(w_start_date_year_product_grou_3).alias("second_premium"),
        F.last("start_date", ignorenulls=True).over(w_start_date_year_product_grou_4).alias("last_known_start_date"),
    )
)

first_rows = with_windows.filter(F.col("end_date_seq") == 1)

metrics = (
    first_rows
    .select(
        "*",
        F.size(F.collect_set("holder_id").over(w_product_rn)).alias("distinct_holder_id_in_product_rn"),
        F.sum("end_date_tercile").over(w_product_rn).alias("product_rn_sum_end_date_tercile"),
        F.last("rn").over(w_end_date_seq_by_prev3_premiu).alias("last_rn"),
        F.min("premium_distance_max_2_prior").over(w_product_rn).alias("product_rn_min_premium_distance_max_2_prior"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_product_rn), F.lit(0)), 4).alias("premium_share_of_product_rn"),
        F.round(F.percent_rank().over(w_end_date_rank_by_start_date), 3).alias("start_date_percent_rank"),
    )
)

result = (
    metrics
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        "product_group",
        "premium_distance",
        "end_date_tercile",
        "premium_rank",
        "prev3_premium_distance",
        "rn",
        "premium_vs_product_avg",
        "second_premium",
        "distinct_holder_id_in_product_rn",
        "product_rn_sum_end_date_tercile",
        "last_rn",
        "start_date_percent_rank",
        F.coalesce(F.col("product_group"), F.lit("UNKNOWN")).alias("product_group_or_unknown"),
    )
    .orderBy(F.col("holder_id").desc(), "end_date_tercile")
)
