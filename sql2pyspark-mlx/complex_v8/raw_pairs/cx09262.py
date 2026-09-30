from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_opened_on_quarter_month = Window.partitionBy(F.date_trunc("month", "opened_on_quarter"))
w_all_by_capacity_full = Window.orderBy("capacity", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_capacity_running = Window.orderBy("capacity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent_warehouses = (
    df_warehouses
    .filter(F.col("capacity").between(4912, 11506))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.dayofweek(F.col("opened_on")).alias("opened_on_dayofweek"),
        F.date_trunc("quarter", F.col("opened_on")).alias("opened_on_quarter"),
    )
)

metrics = (
    recent_warehouses
    .select(
        "*",
        F.size(F.collect_set("city").over(w_opened_on_quarter_month)).alias("distinct_city_in_opened_on_quarter_month"),
        F.nth_value("opened_on", 2).over(w_all_by_capacity_full).alias("second_opened_on"),
        F.min("capacity").over(w_all_by_capacity_running).alias("min_capacity_so_far"),
    )
)

with_activity = (
    metrics.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.move_type") != "ADJUST") & (F.col("st.unit_price") > 180.0), "left_anti")
    .filter(F.col("opened_on_quarter") < F.lit("2025-02-17 09:00:00").cast("timestamp"))
)

result = (
    with_activity
    .filter(F.col("capacity") >= 2614)
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        "opened_on_dayofweek",
        "opened_on_quarter",
        "distinct_city_in_opened_on_quarter_month",
        "second_opened_on",
        F.quarter(F.col("second_opened_on")).alias("second_opened_on_quarter"),
    )
    .orderBy("distinct_city_in_opened_on_quarter_month", "id")
    .limit(5)
)
