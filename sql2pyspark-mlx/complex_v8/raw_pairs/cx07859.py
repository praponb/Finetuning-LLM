from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_holder_segment_holder_id = Window.partitionBy("holder_segment", "holder_id")
w_holder_segment_end_date_year = Window.partitionBy("holder_segment", F.year("end_date"))
w_all_by_premium = Window.orderBy(F.col("premium").asc_nulls_last(), "id")
w_all_by_premium_moving_1 = Window.orderBy(F.col("premium").desc(), "id").rowsBetween(-1, Window.currentRow)
w_end_date_month_by_premium = Window.partitionBy(F.date_trunc("month", "end_date")).orderBy("premium", "id")
w_holder_id_holder_segment_by = Window.partitionBy("holder_id", "holder_segment").orderBy("premium", "id")
w_holder_id_holder_segment_by_2 = Window.partitionBy("holder_id", "holder_segment").orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base_policys = (
    df_policies
    .filter((F.col("status").isin("ACTIVE", "CANCELLED", "EXPIRED")) & (F.col("start_date") >= F.lit("2024-11-20").cast("date")))
    .select(
        "id",
        "holder_id",
        "product",
        "premium",
        "end_date",
        F.concat(F.col("product"), F.lit("#"), F.col("id").cast("string")).alias("product_ref"),
    )
)

joined_holder = (
    base_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "inner")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.end_date",
        "s.product_ref",
        "p.holder_name",
        F.col("p.segment").alias("holder_segment"),
    )
)

with_windows = (
    joined_holder
    .select(
        "*",
        F.count(F.lit(1)).over(w_holder_segment_holder_id).alias("rows_in_holder_segment_holder_id"),
        F.sum("premium").over(w_holder_segment_end_date_year).alias("holder_segment_end_date_year_sum_premium"),
        F.lag("premium", 2).over(w_all_by_premium).alias("prev2_premium"),
        F.max("premium").over(w_all_by_premium_moving_1).alias("premium_max_2"),
        F.ntile(5).over(w_all_by_premium).alias("premium_quintile"),
        F.datediff(F.col("end_date"), F.lag("end_date").over(w_end_date_month_by_premium)).alias("days_since_prev_end_date"),
        (F.col("premium") - F.lag("premium").over(w_holder_id_holder_segment_by)).alias("premium_change"),
        F.rank().over(w_holder_id_holder_segment_by).alias("premium_rank"),
        F.max("premium").over(w_holder_id_holder_segment_by_2).alias("max_premium_so_far"),
        F.row_number().over(w_all_by_premium).alias("premium_seq"),
    )
)

labeled = (
    with_windows
    .select(
        "*",
        F.last_day(F.col("end_date")).alias("end_date_month_end"),
        F.coalesce(F.col("holder_segment"), F.lit("UNKNOWN")).alias("holder_segment_or_unknown"),
        (F.col("prev2_premium") - F.col("premium_quintile")).alias("prev2_premium_minus_premium_quintile"),
    )
)

stats = (
    labeled
    .groupBy(
        "holder_id",
        "holder_segment",
        F.date_trunc("month", "end_date_month_end").alias("end_date_month_end_month"),
    )
    .agg(
        F.array_sort(F.collect_set("holder_segment")).alias("holder_segment_list"),
        F.round(F.stddev("max_premium_so_far"), 2).alias("stddev_max_premium_so_far"),
        F.datediff(F.max("end_date_month_end"), F.min("end_date_month_end")).alias("end_date_month_end_span_days"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.upper(F.trim(F.col("holder_name"))).like("M%"), 1).otherwise(0)).alias("n_upper_trim_holder_name_like"),
    )
)

result = (
    stats
    .select(
        "holder_id",
        "holder_segment_list",
        "stddev_max_premium_so_far",
        "end_date_month_end_span_days",
        "row_count",
        "n_upper_trim_holder_name_like",
    )
)
