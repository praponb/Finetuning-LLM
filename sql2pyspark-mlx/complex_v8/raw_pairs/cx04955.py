from pyspark.sql import functions as F

scoped = (
    df_warehouses
    .filter((F.col("city") != "Kuala Lumpur") & (F.col("capacity") > 4912))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.concat_ws(" / ", F.col("city"), F.col("wh_code")).alias("city_wh_code_label"),
        F.lower(F.regexp_replace(F.trim(F.col("wh_code")), " ", "_")).alias("wh_code_slug"),
        F.date_trunc("month", F.col("opened_on")).alias("opened_on_month"),
    )
)

filtered = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.move_type") == "TRANSFER"), "left_anti")
)

with_children = (
    filtered.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.unit_price") >= 150.7), "inner")
    .select(
        "s.id",
        "s.capacity",
        "s.opened_on_month",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.unit_price").alias("move_unit_price"),
        "st.move_type",
    )
)

with_activity = (
    with_children.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (~F.col("st.operator").isin("op_dew", "op_eve")), "left_anti")
)

result = (
    with_activity
    .groupingSets(
        [["move_type", "move_id"], ["move_type"], ["move_id"], []],
        "move_type", "move_id",
    )
    .agg(
        F.grouping("move_type").alias("is_total_move_type"),
        F.round(F.stddev("move_unit_price"), 2).alias("stddev_move_unit_price"),
        F.max("capacity").alias("max_capacity"),
    )
    .select(
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "move_id",
        "is_total_move_type",
        "stddev_move_unit_price",
        "max_capacity",
    )
    .orderBy(F.col("move_type").asc_nulls_last(), F.col("move_id").asc_nulls_last())
)
