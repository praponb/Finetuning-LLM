from pyspark.sql import functions as F

recent = (
    df_warehouses
    .filter(F.col("city") == "Kuala Lumpur")
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        (F.floor(F.col("capacity") / 5000) * 5000).alias("capacity_bucket"),
    )
)

derived = (
    recent
    .filter(F.col("capacity_bucket") >= 2900)
    .select(
        "*",
        F.date_trunc("quarter", F.col("opened_on")).alias("opened_on_quarter"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
        F.max("qty").alias("max_move_qty"),
    )
)

summarized_rows = (
    derived.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select("s.*", "st.avg_move_qty", "st.max_move_qty")
)

result = (
    summarized_rows
    .select(
        "id",
        "max_move_qty",
        "capacity",
        "capacity_bucket",
    )
    .unpivot(
        ids=["id"],
        values=["max_move_qty", "capacity", "capacity_bucket"],
        variableColumnName="measure",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
