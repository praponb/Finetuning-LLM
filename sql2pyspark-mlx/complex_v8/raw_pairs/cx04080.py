from pyspark.sql import functions as F

filtered_warehouses = (
    df_warehouses
    .filter((F.col("capacity") < 11768) & (F.upper(F.trim(F.col("wh_code"))).like("K%")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
    )
)

annotated = (
    filtered_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("B%"))
    .withColumn("city_len", F.length(F.trim(F.col("city"))))
)

exploded_detail = (
    annotated.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.id"), "left")
    .select(
        "s.id",
        "s.wh_code",
        "s.capacity",
        "s.opened_on",
        F.col("st.id").alias("move_id"),
        "st.move_type",
        F.col("st.qty").alias("move_qty"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    exploded_detail
    .groupingSets(
        [["move_id", "move_sku_id", "move_type"], ["move_id"], []],
        "move_id", "move_sku_id", "move_type",
    )
    .agg(
        F.countDistinct("move_sku_id").alias("distinct_move_sku_ids"),
        F.datediff(F.max("opened_on"), F.min("opened_on")).alias("opened_on_span_days"),
        F.round(100 * F.count(F.when(F.col("move_type") == "OUTBOUND", 1)) / F.count("*"), 1).alias("pct_move_type_outbound"),
    )
    .orderBy(
        F.col("move_id").asc_nulls_last(),
        F.col("move_sku_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
