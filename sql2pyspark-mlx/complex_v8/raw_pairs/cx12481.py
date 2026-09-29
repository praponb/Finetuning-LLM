from pyspark.sql import functions as F

valid = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("B%"))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

derived = (
    valid
    .filter(F.upper(F.trim(F.col("wh_code"))).like("C%"))
    .withColumn("capacity_tier", F.when(F.col("capacity") >= 11506, "HIGH").when(F.col("capacity") >= 11005, "MEDIUM").otherwise("LOW"))
)

qualified = (
    derived.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.col("st.unit_price") <= 313.8), "left_anti")
)

line_level = (
    qualified.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.id")) & (F.hour(F.col("st.moved_at")).between(7, 13)), "left")
    .select(
        "s.id",
        "s.wh_code",
        "s.city",
        "s.opened_on",
        "s.capacity_tier",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

result = (
    line_level
    .groupBy("capacity_tier")
    .pivot("city", ["Singapore", "Bangkok"])
    .agg(F.max("move_qty"))
    .orderBy("capacity_tier")
)
