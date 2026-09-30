from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_last_last_moved_at_month_n_c = Window.partitionBy(F.date_trunc("month", "last_last_moved_at"), "n_city_kuala_lumpur")
w_all_by_stddev_days_since_ope = Window.orderBy("stddev_days_since_opened_on", "last_moved_at_ym", "opened_on_dayofweek").rowsBetween(-1, -1)
w_n_city_kuala_lumpur_by_capac = Window.partitionBy("n_city_kuala_lumpur").orderBy("capacity_to_capacity_bucket_ratio", "last_moved_at_ym", "opened_on_dayofweek").rowsBetween(-6, -1)
w_all_by_stddev_days_since_ope_2 = Window.orderBy("stddev_days_since_opened_on", "last_moved_at_ym", "opened_on_dayofweek")
w_n_city_kuala_lumpur_by_capac_2 = Window.partitionBy("n_city_kuala_lumpur").orderBy("capacity_to_capacity_bucket_ratio")

cleaned = (
    df_warehouses
    .filter(F.col("capacity") >= 2614)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
        F.dayofweek(F.col("opened_on")).alias("opened_on_dayofweek"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.max("qty").alias("max_move_qty"),
        F.count("*").alias("move_count"),
    )
)

summarized_rows = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.max_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

per_group = (
    summarized_rows
    .groupBy(
        F.date_format("last_moved_at", "yyyy-MM").alias("last_moved_at_ym"),
        "opened_on_dayofweek",
    )
    .agg(
        F.max("last_moved_at").alias("last_last_moved_at"),
        F.datediff(F.max("opened_on"), F.min("opened_on")).alias("opened_on_span_days"),
        F.count(F.when(F.col("city") == "Kuala Lumpur", 1)).alias("n_city_kuala_lumpur"),
        F.round(F.stddev("days_since_opened_on"), 2).alias("stddev_days_since_opened_on"),
        F.round(F.sum("capacity") / F.nullif(F.sum("capacity_bucket"), F.lit(0)), 3).alias("capacity_to_capacity_bucket_ratio"),
    )
)

metrics = (
    per_group
    .select(
        "last_moved_at_ym",
        "opened_on_dayofweek",
        "last_last_moved_at",
        F.sum("opened_on_dayofweek").over(w_all_by_stddev_days_since_ope).alias("opened_on_dayofweek_sum_1_prior"),
        F.sum("stddev_days_since_opened_on").over(w_n_city_kuala_lumpur_by_capac).alias("stddev_days_since_opened_on_sum_6_prior"),
        F.ntile(4).over(w_all_by_stddev_days_since_ope_2).alias("stddev_days_since_opened_on_quartile"),
        F.round(F.cume_dist().over(w_n_city_kuala_lumpur_by_capac_2), 3).alias("capacity_to_capacity_bucket_ratio_cume_dist"),
    )
)

with_activity = (
    metrics
    .crossJoin(metrics.agg(F.avg("stddev_days_since_opened_on_quartile").alias("overall_avg_stddev_days_since_opened_on_quartile")))
    .filter(F.col("stddev_days_since_opened_on_quartile") > F.col("overall_avg_stddev_days_since_opened_on_quartile"))
    .drop("overall_avg_stddev_days_since_opened_on_quartile")
)

result = (
    with_activity
    .select(
        "last_moved_at_ym",
        "opened_on_dayofweek",
        "opened_on_dayofweek_sum_1_prior",
        "stddev_days_since_opened_on_sum_6_prior",
        "stddev_days_since_opened_on_quartile",
        "capacity_to_capacity_bucket_ratio_cume_dist",
        F.upper(F.trim(F.col("last_moved_at_ym"))).alias("last_moved_at_ym_clean"),
        F.when(F.col("opened_on_dayofweek") >= 6, "HIGH").when(F.col("opened_on_dayofweek") >= 2, "MEDIUM").otherwise("LOW").alias("opened_on_dayofweek_tier"),
    )
    .orderBy(
        F.col("last_moved_at_ym").asc_nulls_last(),
        F.col("opened_on_dayofweek_sum_1_prior").desc(),
    )
)
