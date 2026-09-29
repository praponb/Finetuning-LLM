from pyspark.sql import functions as F

filtered = (
    df_stock_moves
    .filter((F.col("warehouse_id") >= 3) & (F.col("operator") == "op_bee"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        F.when(F.col("operator").isin("op_dew", "op_bee"), "group_a").when(F.col("operator") == "op_ann", "group_b").otherwise("other").alias("operator_group"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_ref"),
    )
)

filtered_2 = (
    filtered
    .crossJoin(filtered.agg(F.min("qty").alias("overall_min_qty")))
    .filter(F.col("qty") > F.col("overall_min_qty"))
    .drop("overall_min_qty")
)

filtered_3 = (
    filtered_2.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") < 323.9) & (F.col("st.move_type") != "OUTBOUND"), "left_anti")
)

result = (
    filtered_3
    .groupingSets(
        [["operator_group", "warehouse_id", "sku_id"], ["operator_group"], ["warehouse_id"], []],
        "operator_group", "warehouse_id", "sku_id",
    )
    .agg(
        F.percentile_approx("qty", 0.5).alias("median_qty"),
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("cnt"),
    )
    .orderBy(
        F.col("operator_group").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
