from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_full = Window.orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_holder_id_holder_region_by_s = Window.partitionBy("holder_id", "holder_region").orderBy("start_date", "id")
w_holder_region_by_start_date = Window.partitionBy("holder_region").orderBy("start_date", "id")
w_holder_region_by_start_date_2 = Window.partitionBy("holder_region").orderBy(F.col("start_date").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_start_date_running = Window.orderBy(F.col("start_date").asc_nulls_last(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_region_by_start_date_3 = Window.partitionBy("holder_region").orderBy(F.col("start_date").desc(), "id")
w_holder_region_seq_no = Window.partitionBy("holder_region", "seq_no")
w_holder_id_by_prev2_start_dat = Window.partitionBy("holder_id").orderBy(F.col("prev2_start_date").asc_nulls_last())

valid_policys = (
    df_policies
    .filter((F.col("product").isin("HOME", "AUTO", "HEALTH")) & (F.col("status").isin("CANCELLED", "EXPIRED", "ACTIVE")))
    .select(
        "id",
        "holder_id",
        "product",
        "start_date",
        "end_date",
        "status",
        F.datediff(F.col("end_date"), F.col("start_date")).alias("days_start_date_to_end_date"),
    )
)

joined = (
    valid_policys.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.start_date",
        "s.status",
        F.col("p.region").alias("holder_region"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.first("start_date").over(w_all_by_start_date_full).alias("first_start_date"),
        F.datediff(F.col("start_date"), F.lag("start_date").over(w_holder_id_holder_region_by_s)).alias("days_since_prev_start_date"),
        F.row_number().over(w_holder_region_by_start_date).alias("rn"),
        F.nth_value("start_date", 2).over(w_holder_region_by_start_date_2).alias("second_start_date"),
        F.last("start_date", ignorenulls=True).over(w_all_by_start_date_running).alias("last_known_start_date"),
        F.lag("start_date", 2).over(w_holder_id_holder_region_by_s).alias("prev2_start_date"),
        F.round(F.percent_rank().over(w_holder_id_holder_region_by_s), 3).alias("start_date_percent_rank"),
        F.row_number().over(w_holder_region_by_start_date_3).alias("seq_no"),
        F.lead("start_date", 2).over(w_holder_id_holder_region_by_s).alias("next2_start_date"),
    )
)

with_windows = (
    with_trends
    .select(
        "id",
        "start_date",
        "status",
        "first_start_date",
        "days_since_prev_start_date",
        "rn",
        "second_start_date",
        "last_known_start_date",
        "prev2_start_date",
        "start_date_percent_rank",
        "seq_no",
        "next2_start_date",
        F.round(F.col("days_since_prev_start_date") - F.avg("days_since_prev_start_date").over(w_holder_region_seq_no), 2).alias("days_since_prev_start_date_vs_holder_region_seq_"),
        F.size(F.collect_set("holder_id").over(w_holder_region_seq_no)).alias("distinct_holder_id_in_holder_region_seq_no"),
        F.max("seq_no").over(w_holder_region_seq_no).alias("holder_region_seq_no_max_seq_no"),
        F.round(F.cume_dist().over(w_holder_id_by_prev2_start_dat), 3).alias("prev2_start_date_cume_dist"),
    )
)

grouped = (
    with_windows
    .filter(F.col("start_date_percent_rank") >= 0.3)
    .groupBy(
        F.date_format("prev2_start_date", "yyyy-MM").alias("prev2_start_date_ym"),
        F.date_format("first_start_date", "yyyy-MM").alias("first_start_date_ym"),
    )
    .agg(
        F.round(F.stddev("rn"), 2).alias("stddev_rn"),
        F.round(F.sum("rn") / F.nullif(F.sum("days_since_prev_start_date"), F.lit(0)), 3).alias("rn_to_days_since_prev_start_date_ratio"),
        F.count(F.when(F.col("status") == "ACTIVE", 1)).alias("n_status_active"),
        F.countDistinct("status").alias("distinct_status"),
    )
)

result = (
    grouped
    .select(
        "prev2_start_date_ym",
        "first_start_date_ym",
        "stddev_rn",
        "rn_to_days_since_prev_start_date_ratio",
        "n_status_active",
        F.lower(F.regexp_replace(F.trim(F.col("prev2_start_date_ym")), " ", "_")).alias("prev2_start_date_ym_slug"),
    )
    .orderBy(F.col("stddev_rn").asc_nulls_last())
)
