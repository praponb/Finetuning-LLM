from pyspark.sql import functions as F

cleaned_moves = (
    df_stock_moves
    .filter((F.col("operator") == "op_dew") & (F.col("move_type").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
    )
)

filtered = (
    cleaned_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") >= 136.0), "left_anti")
)

totals = (
    filtered
    .filter(F.col("greatest_unit_price_qty") > 125.8)
    .groupBy("sku_id")
    .agg(
        F.max("unit_price").alias("max_unit_price"),
        F.percentile_approx("greatest_unit_price_qty", 0.5).alias("median_greatest_unit_price_qty"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.round(F.sum("greatest_unit_price_qty"), 2).alias("total_greatest_unit_price_qty"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
)

annotated = (
    totals
    .select(
        "sku_id",
        "max_unit_price",
        "move_type_list",
        "distinct_move_types",
        "operator_list",
        F.size(F.col("move_type_list")).alias("move_type_list_count"),
        (F.col("distinct_move_types") - F.col("max_unit_price")).alias("distinct_move_types_minus_max_unit_price"),
    )
)

result = (
    annotated
    .select("sku_id", "move_type_list", "distinct_move_types")
    .orderBy(F.col("sku_id").asc_nulls_last(), "distinct_move_types")
    .limit(5)
)
