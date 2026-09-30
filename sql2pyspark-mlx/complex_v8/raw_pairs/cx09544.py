from pyspark.sql import functions as F

valid = (
    df_skus
    .filter((~F.col("is_active")) | (F.col("unit_cost").between(134.3, 251.2)))
    .select(
        "id",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
    )
)

qualified = (
    valid
    .crossJoin(valid.agg(F.avg("unit_cost").alias("overall_avg_unit_cost")))
    .filter(F.col("unit_cost") > F.col("overall_avg_unit_cost"))
    .drop("overall_avg_unit_cost")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator") == "op_eve", 1)).alias("move_operator_count"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_stats = (
    qualified.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        "st.last_moved_at",
    )
)

filtered = (
    with_stats.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (~F.col("st.operator").isin("op_chan", "op_dew")) & (F.col("st.move_type") == "OUTBOUND"), "left_semi")
    .filter(F.size(F.col("labels")) >= 1)
)

stats = (
    filtered
    .groupBy(
        "is_active",
        F.year("last_moved_at").alias("last_moved_at_year"),
        "move_operator_count",
    )
    .agg(
        F.min("move_operator_count").alias("min_move_operator_count"),
        F.count("*").alias("cnt"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("unit_cost", 0.5).alias("median_unit_cost"),
        F.min("last_moved_at").alias("first_last_moved_at"),
        F.round(F.sum("move_operator_count") / F.nullif(F.sum("weight_kg"), F.lit(0)), 3).alias("move_operator_count_to_weight_kg_ratio"),
    )
)

annotated = (
    stats
    .select(
        "is_active",
        "last_moved_at_year",
        "move_operator_count",
        "min_move_operator_count",
        "distinct_ids",
        "median_unit_cost",
        "first_last_moved_at",
        "move_operator_count_to_weight_kg_ratio",
        F.quarter(F.col("first_last_moved_at")).alias("first_last_moved_at_quarter"),
    )
)

result = (
    annotated
    .select(
        "is_active",
        "last_moved_at_year",
        "move_operator_count",
        "min_move_operator_count",
        "distinct_ids",
        "move_operator_count_to_weight_kg_ratio",
    )
    .orderBy(
        F.col("move_operator_count_to_weight_kg_ratio").asc_nulls_last(),
        F.col("distinct_ids").asc(),
    )
)
