from pyspark.sql import functions as F

base = (
    df_skus
    .filter((F.col("unit_cost") < 277.0) | (F.size(F.col("labels")) >= 1))
    .select(
        "id",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("unit_cost") >= 251.2, "A").when(F.col("unit_cost") >= 164.5, "B").otherwise("C").alias("unit_cost_tier"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator").isNotNull(), 1)).alias("move_operator_count"),
        F.max("unit_price").alias("max_move_unit_price"),
    )
)

summarized_rows = (
    base.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        "st.max_move_unit_price",
    )
)

filtered = (
    summarized_rows
    .crossJoin(summarized_rows.agg(F.min("max_move_unit_price").alias("overall_min_max_move_unit_price")))
    .filter(F.col("max_move_unit_price") > F.col("overall_min_max_move_unit_price"))
    .drop("overall_min_max_move_unit_price")
)

result = (
    filtered
    .select(
        "id",
        "unit_cost",
        "weight_kg",
        "is_active",
        "unit_cost_tier",
        "move_operator_count",
        "max_move_unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("unit_cost_tier")), " ", "_")).alias("unit_cost_tier_slug"),
    )
    .orderBy(F.col("move_operator_count").desc())
)
