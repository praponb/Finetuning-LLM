from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("capacity_tier").orderBy(F.col("opened_on").desc(), F.col("id").desc())
w_opened_on_month = Window.partitionBy(F.month("opened_on"))
w_all_by_capacity_running = Window.orderBy(F.col("capacity").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_capacity_filled = Window.orderBy(F.col("capacity_filled").desc(), "id")

scoped = (
    df_warehouses
    .filter(F.col("city") != "Singapore")
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.date_format(F.col("opened_on"), "yyyy-MM-dd").alias("opened_on_label"),
    )
)

scored = (
    scoped
    .select(
        "id",
        "capacity",
        "opened_on",
        "opened_on_label",
        F.when(F.col("capacity") >= 11506, "HIGH").when(F.col("capacity") >= 4912, "MEDIUM").otherwise("LOW").alias("capacity_tier"),
        F.coalesce(F.col("capacity"), F.lit(0)).alias("capacity_filled"),
    )
)

latest_per_key = (
    scored
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

screened = (
    latest_per_key.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.operator").isNotNull()) & (F.hour(F.col("st.moved_at")).between(10, 12)), "left_anti")
)

with_windows = (
    screened
    .select(
        "id",
        "capacity",
        "opened_on_label",
        "capacity_tier",
        "capacity_filled",
        F.sum("capacity").over(w_opened_on_month).alias("opened_on_month_sum_capacity"),
        F.round(F.avg("capacity").over(w_opened_on_month), 2).alias("opened_on_month_avg_capacity"),
        F.round(F.col("capacity") - F.avg("capacity").over(w_opened_on_month), 2).alias("capacity_vs_opened_on_month_avg"),
        F.last("opened_on", ignorenulls=True).over(w_all_by_capacity_running).alias("last_known_opened_on"),
        F.lead("capacity").over(w_all_by_capacity_filled).alias("next_capacity"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("move_count"),
    )
)

profiled = (
    with_windows.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

result = (
    profiled
    .select(
        "id",
        "capacity",
        "opened_on_label",
        "capacity_tier",
        "opened_on_month_sum_capacity",
        "opened_on_month_avg_capacity",
        "capacity_vs_opened_on_month_avg",
        "last_known_opened_on",
        "next_capacity",
        "first_moved_at",
        "move_count",
        F.dayofweek(F.col("first_moved_at")).alias("first_moved_at_dayofweek"),
        F.coalesce(F.col("capacity_tier"), F.lit("UNKNOWN")).alias("capacity_tier_or_unknown"),
    )
    .orderBy(
        F.col("opened_on_month_sum_capacity").asc_nulls_last(),
        F.col("id").asc_nulls_last(),
    )
    .limit(20)
)
