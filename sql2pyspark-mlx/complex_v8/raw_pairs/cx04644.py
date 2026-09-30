from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_city_by_opened_on_month_runn = Window.partitionBy("city").orderBy("opened_on_month", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_city_by_opened_on_month = Window.partitionBy("city").orderBy("opened_on_month", "id")
w_opened_on_month_city = Window.partitionBy("opened_on_month", "city")
w_city_by_opened_on_month_movi = Window.partitionBy("city").orderBy("opened_on_month", "id").rowsBetween(-1, Window.currentRow)
w_city_by_opened_on_month_full = Window.partitionBy("city").orderBy("opened_on_month", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

scoped_warehouses = (
    df_warehouses
    .filter(F.col("wh_code").isNotNull())
    .select(
        "id",
        "city",
        "capacity",
        "opened_on",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
        F.month(F.col("opened_on")).alias("opened_on_month"),
    )
)

sequenced = (
    scoped_warehouses
    .select(
        "*",
        F.first("opened_on", ignorenulls=True).over(w_city_by_opened_on_month_runn).alias("first_known_opened_on"),
        F.row_number().over(w_city_by_opened_on_month).alias("seq_no"),
        F.min("capacity").over(w_opened_on_month_city).alias("opened_on_month_city_min_capacity"),
        F.round(F.avg("days_since_opened_on").over(w_city_by_opened_on_month_movi), 2).alias("days_since_opened_on_avg_2"),
        F.nth_value("days_since_opened_on", 2).over(w_city_by_opened_on_month_full).alias("second_days_since_opened_on"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

with_stats = (
    sequenced.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
        "st.first_moved_at",
    )
)

result = (
    with_stats
    .select(
        "id",
        "city",
        "capacity",
        "days_since_opened_on",
        "first_known_opened_on",
        "seq_no",
        "opened_on_month_city_min_capacity",
        "days_since_opened_on_avg_2",
        "second_days_since_opened_on",
        "move_count_2",
    )
    .orderBy(F.col("move_count_2").asc_nulls_last())
)
