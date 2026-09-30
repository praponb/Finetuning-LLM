from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_id = Window.partitionBy("holder_id")
w_all_by_start_date = Window.orderBy("start_date", "id")
w_holder_id_holder_segment_by = Window.partitionBy("holder_id", "holder_segment").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_start_date_month = Window.partitionBy("holder_id", F.month("start_date"))
w_product_holder_segment_by_st = Window.partitionBy("product", "holder_segment").orderBy("start_date", "id")
w_holder_id_holder_segment_by_2 = Window.partitionBy("holder_id", "holder_segment").orderBy("premium", "id")
w_holder_id_holder_segment_by_3 = Window.partitionBy("holder_id", "holder_segment").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_segment_by_start_date = Window.partitionBy("holder_segment").orderBy("start_date", "id")
w_holder_segment = Window.partitionBy("holder_segment")
w_holder_id_holder_segment_by_4 = Window.partitionBy("holder_id", "holder_segment").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_policys = (
    df_policies
    .filter((F.col("holder_id") <= 17) & (F.col("product") == "LIFE"))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "end_date",
        "status",
    )
)

joined = (
    cleaned_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .filter(~F.col("p.segment").isin("SME", "VIP"))
    .select(
        "s.id",
        "s.holder_id",
        "s.product",
        "s.premium",
        "s.start_date",
        F.col("p.segment").alias("holder_segment"),
    )
)

with_windows = (
    joined
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        F.max("premium").over(w_holder_id).alias("holder_id_max_premium"),
        F.ntile(3).over(w_all_by_start_date).alias("start_date_tercile"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id), 2).alias("premium_vs_holder_id_avg"),
        F.count(F.lit(1)).over(w_holder_id_holder_segment_by).alias("running_count"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_start_date_month), 2).alias("premium_vs_holder_id_start_date_month_avg"),
        F.lag("start_date").over(w_product_holder_segment_by_st).alias("prev_start_date"),
        F.lag("start_date", 3).over(w_holder_id_holder_segment_by_2).alias("prev3_start_date"),
        F.first("premium", ignorenulls=True).over(w_holder_id_holder_segment_by_3).alias("first_known_premium"),
        F.dense_rank().over(w_holder_segment_by_start_date).alias("start_date_dense_rank"),
        F.sum("premium").over(w_holder_segment).alias("holder_segment_sum_premium"),
        F.last("start_date").over(w_holder_id_holder_segment_by_4).alias("last_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_holder_segment_by_2)).alias("days_since_prev_start_date"),
        F.count(F.lit(1)).over(w_holder_segment).alias("rows_in_holder_segment"),
        F.count(F.lit(1)).over(w_holder_id_start_date_month).alias("rows_in_holder_id_start_date_month"),
        F.round(F.percent_rank().over(w_product_holder_segment_by_st), 3).alias("start_date_percent_rank"),
    )
)

top_per_group = (
    with_windows
    .filter(F.col("start_date_dense_rank") <= 5)
    .drop("start_date_dense_rank")
)

result = (
    top_per_group
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "start_date",
        "holder_id_max_premium",
        "running_count",
        "premium_vs_holder_id_start_date_month_avg",
        "prev_start_date",
        "prev3_start_date",
        "holder_segment_sum_premium",
        "last_start_date",
        "rows_in_holder_segment",
        "rows_in_holder_id_start_date_month",
        "start_date_percent_rank",
        F.abs(F.col("rows_in_holder_segment") - 10).alias("rows_in_holder_segment_distance"),
    )
    .orderBy(F.col("rows_in_holder_id_start_date_month").desc_nulls_first())
)
