from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_start_date_ties = Window.orderBy("start_date")
w_all_by_premium_running = Window.orderBy("premium", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_start_date_running = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_holder_id_holder_segment = Window.partitionBy("holder_id", "holder_segment")
w_all_by_start_date_prior_6 = Window.orderBy(F.col("start_date").desc(), "id").rowsBetween(-6, -1)
w_all_by_start_date = Window.orderBy("start_date", "id")
w_all_by_premium_ties = Window.orderBy("premium")
w_all_by_premium = Window.orderBy("premium", "id")
w_status_by_premium_decile_sta = Window.partitionBy("status").orderBy("premium_decile_status_is_not_null", "status")
w_status_by_premium_decile_sta_2 = Window.partitionBy("status").orderBy("premium_decile_status_is_not_null", "status").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_policies
    .filter((F.col("status") == "CANCELLED") & (F.col("product") == "HOME"))
    .select(
        "id",
        "holder_id",
        "premium",
        "start_date",
        "status",
        F.substring(F.trim(F.col("product")), 1, 3).alias("product_prefix"),
        F.when(F.col("product").isin("HEALTH", "TRAVEL"), "group_a").when(F.col("product") == "HOME", "group_b").otherwise("other").alias("product_group"),
    )
)

joined = (
    cleaned.alias("s")
    .join(df_policyholders.alias("p"), F.col("p.id") == F.col("s.holder_id"), "left")
    .select(
        "s.id",
        "s.holder_id",
        "s.premium",
        "s.start_date",
        "s.status",
        "s.product_prefix",
        "s.product_group",
        F.col("p.segment").alias("holder_segment"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.rank().over(w_all_by_start_date_ties).alias("start_date_rank"),
        F.count(F.lit(1)).over(w_all_by_premium_running).alias("running_count"),
        F.last("start_date", ignorenulls=True).over(w_all_by_start_date_running).alias("last_known_start_date"),
        F.sum("premium").over(w_holder_id_holder_segment).alias("holder_id_holder_segment_sum_premium"),
        F.round(F.col("premium") - F.avg("premium").over(w_holder_id_holder_segment), 2).alias("premium_vs_holder_id_holder_segment_avg"),
        F.count(F.lit(1)).over(w_holder_id_holder_segment).alias("rows_in_holder_id_holder_segment"),
        F.min("premium").over(w_holder_id_holder_segment).alias("holder_id_holder_segment_min_premium"),
        F.round(F.avg("premium").over(w_all_by_start_date_prior_6), 2).alias("premium_avg_6_prior"),
        F.row_number().over(w_all_by_start_date).alias("start_date_seq"),
        F.round(F.col("premium") / F.nullif(F.sum("premium").over(w_holder_id_holder_segment), F.lit(0)), 4).alias("premium_share_of_holder_id_holder_segment"),
        F.rank().over(w_all_by_premium_ties).alias("premium_rank"),
        F.ntile(10).over(w_all_by_premium).alias("premium_decile"),
        (F.col("premium") - F.lag("premium").over(w_all_by_start_date)).alias("premium_change"),
        F.ntile(3).over(w_all_by_start_date).alias("start_date_tercile"),
    )
)

top_per_group = sequenced.filter((F.col("premium_rank") == 1) & (F.col("premium_change") < 1500.0))

grouped = (
    top_per_group
    .filter(F.col("start_date_seq").between(2, 7))
    .groupBy("status")
    .agg(
        F.max("last_known_start_date").alias("last_last_known_start_date"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("status").isNotNull(), F.col("premium_decile"))).alias("premium_decile_status_is_not_null"),
    )
    .filter(F.col("cnt") >= 3)
)

metrics = (
    grouped
    .select(
        "status",
        "cnt",
        "premium_decile_status_is_not_null",
        F.round(F.cume_dist().over(w_status_by_premium_decile_sta), 3).alias("premium_decile_status_is_not_null_cume_dist"),
        F.first("cnt", ignorenulls=True).over(w_status_by_premium_decile_sta_2).alias("first_known_cnt"),
    )
)

result = (
    metrics
    .filter(F.col("premium_decile_status_is_not_null") >= 10)
    .select(
        "status",
        "cnt",
        "premium_decile_status_is_not_null_cume_dist",
        "first_known_cnt",
    )
    .orderBy(F.col("first_known_cnt").asc_nulls_last())
)
