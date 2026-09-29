from pyspark.sql import functions as F

valid_warehouses = (
    df_warehouses
    .filter(F.col("capacity") < 11506)
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.lower(F.regexp_replace(F.trim(F.col("city")), " ", "_")).alias("city_slug"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.sum("unit_price").alias("total_move_unit_price"),
        F.count("*").alias("move_count"),
    )
)

profiled = (
    valid_warehouses.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        "st.total_move_unit_price",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

result = (
    profiled
    .groupingSets(
        [["move_count", "city"], ["move_count"], ["city"], []],
        "move_count", "city",
    )
    .agg(
        F.datediff(F.max("opened_on"), F.min("opened_on")).alias("opened_on_span_days"),
        F.max("first_moved_at").alias("last_first_moved_at"),
    )
    .orderBy(F.col("move_count").asc_nulls_last(), F.col("city").asc_nulls_last())
)
