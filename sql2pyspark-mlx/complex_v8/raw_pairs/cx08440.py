from pyspark.sql import functions as F

prepared = (
    df_warehouses
    .filter((~F.col("city").isin("Singapore", "Bangkok")) & (F.lower(F.col("wh_code")).like("%ku%")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.abs(F.col("capacity") - 11506).alias("capacity_distance"),
    )
)

screened = (
    prepared
    .crossJoin(prepared.agg(F.avg("capacity").alias("overall_avg_capacity")))
    .filter(F.col("capacity") > F.col("overall_avg_capacity"))
    .drop("overall_avg_capacity")
)

exploded_detail = (
    screened.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.month(F.col("st.moved_at")).isin(1, 3)), "left")
    .select(
        "s.id",
        "s.wh_code",
        "s.city",
        "s.capacity",
        "s.opened_on",
        "s.capacity_distance",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.moved_at").alias("move_moved_at"),
        "st.move_type",
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

result = (
    exploded_detail
    .select(
        "id",
        "city",
        "opened_on",
        "move_id",
        "move_unit_price",
        "move_moved_at",
        "move_type",
        "move_sku_id",
    )
    .orderBy("opened_on")
)
