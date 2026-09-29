from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_row_count = Window.orderBy("row_count", "move_count", "move_move_type_count")
w_move_count_row_count = Window.partitionBy("move_count", "row_count")
w_all_by_row_count_prior_6 = Window.orderBy("row_count", "move_count", "move_move_type_count").rowsBetween(-6, -1)

valid = (
    df_skus
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count(F.when(F.col("move_type").isin("TRANSFER", "OUTBOUND"), 1)).alias("move_move_type_count"),
    )
)

profiled = (
    valid.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

exploded = (
    profiled
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "move_count",
        "move_move_type_count",
        F.posexplode("labels").alias("label_pos", "label"),
    )
)

filtered = (
    exploded.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.move_type").isNotNull()) & (F.col("st.operator") == "op_eve"), "left_anti")
)

totals = (
    filtered
    .groupBy("move_count", "move_move_type_count")
    .agg(
        F.count(F.when(F.col("sku_code").isNotNull(), 1)).alias("n_sku_code_is_not_null"),
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.min("label_pos").alias("min_label_pos"),
        F.count("*").alias("row_count"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.round(F.sum("unit_cost") / F.nullif(F.sum("weight_kg"), F.lit(0)), 3).alias("unit_cost_to_weight_kg_ratio"),
    )
)

with_trends = (
    totals
    .select(
        "*",
        F.ntile(5).over(w_all_by_row_count).alias("row_count_quintile"),
        F.max("move_move_type_count").over(w_move_count_row_count).alias("move_count_row_count_max_move_move_type_count"),
        F.lag("row_count").over(w_all_by_row_count).alias("prev_row_count"),
        F.round(F.avg("unit_cost_to_weight_kg_ratio").over(w_all_by_row_count_prior_6), 2).alias("unit_cost_to_weight_kg_ratio_avg_6_prior"),
    )
)

result = (
    with_trends
    .filter(F.size(F.col("category_list")) >= 1)
    .select(
        "move_count",
        "n_sku_code_is_not_null",
        "category_list",
        "min_label_pos",
        "sku_code_list",
        "unit_cost_to_weight_kg_ratio",
        "row_count_quintile",
        "unit_cost_to_weight_kg_ratio_avg_6_prior",
    )
    .orderBy(F.col("unit_cost_to_weight_kg_ratio_avg_6_prior").desc_nulls_first())
)
