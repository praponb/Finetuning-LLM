from pyspark.sql import functions as F

prepared_moves = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_ann", "op_dew"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
        F.abs(F.col("qty") - 256).alias("qty_distance"),
    )
)

tagged = (
    prepared_moves
    .filter(F.col("move_type") == "TRANSFER")
    .select(
        "*",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.coalesce(F.col("qty_distance"), F.lit(0)).alias("qty_distance_filled"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

result = (
    tagged
    .groupingSets(
        [["move_type_len", "move_type"], ["move_type_len"], ["move_type"], []],
        "move_type_len", "move_type",
    )
    .agg(
        F.grouping_id().alias("gid"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
    .orderBy(
        F.col("move_type_len").asc_nulls_last(),
        F.col("move_type").asc_nulls_last(),
    )
)
