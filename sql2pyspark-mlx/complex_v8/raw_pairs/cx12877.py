from pyspark.sql import functions as F

valid = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

scored = (
    valid
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        "move_type_operator_label",
        F.abs(F.col("qty") - 168).alias("qty_distance"),
        F.abs(F.col("unit_price") - 265.0).alias("unit_price_distance"),
    )
)

qualified = (
    scored
    .crossJoin(scored.agg(F.max("qty").alias("overall_max_qty")))
    .filter(F.col("qty") < F.col("overall_max_qty"))
    .drop("overall_max_qty")
)

result = (
    qualified
    .groupingSets(
        [["operator", "sku_id", "move_type"], ["operator"], ["sku_id"]],
        "operator", "sku_id", "move_type",
    )
    .agg(
        F.grouping("operator").alias("is_total_operator"),
        F.count(F.when(F.col("move_type") == "TRANSFER", 1)).alias("n_move_type_transfer"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.round(F.stddev("qty"), 2).alias("stddev_qty"),
    )
    .orderBy(
        F.col("operator").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
