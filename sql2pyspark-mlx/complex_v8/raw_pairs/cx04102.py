from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_cost = Window.orderBy("unit_cost", "id")
w_is_active_label_is_active = Window.partitionBy("is_active_label", "is_active")
w_all_by_unit_cost_ties = Window.orderBy("unit_cost")

scoped = (
    df_skus
    .filter((F.col("sku_code").isNotNull()) & (F.col("category") == "Toys"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
        F.round(F.col("weight_kg") * 0.25, 2).alias("weight_kg_adj"),
        F.array_join(F.array_sort(F.col("labels")), ",").alias("labels_text"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.col("st.qty") >= 67), "left_anti")
)

enriched = (
    without_matches
    .filter(F.col("unit_cost") >= 108.5)
    .select(
        "*",
        F.abs(F.col("weight_kg_adj") - 22.5).alias("weight_kg_adj_distance"),
        F.when(F.col("is_active"), 1).otherwise(0).alias("is_active_flag"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_counts = (
    enriched.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select("s.*", "st.first_moved_at", "st.last_moved_at")
)

without_matches_2 = (
    with_counts.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left_semi")
)

metrics = (
    without_matches_2
    .select(
        "*",
        F.round(F.percent_rank().over(w_all_by_unit_cost), 3).alias("unit_cost_percent_rank"),
        F.lead("weight_kg", 2).over(w_all_by_unit_cost).alias("next2_weight_kg"),
        F.count(F.lit(1)).over(w_is_active_label_is_active).alias("rows_in_is_active_label_is_active"),
        F.round(F.percent_rank().over(w_all_by_unit_cost_ties), 3).alias("unit_cost_percent_rank_2"),
        F.round(F.cume_dist().over(w_all_by_unit_cost_ties), 3).alias("unit_cost_cume_dist"),
    )
)

result = (
    metrics
    .filter(F.col("unit_cost_cume_dist") > 0.5)
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "labels",
        "is_active",
        "is_active_label",
        "weight_kg_adj",
        "is_active_flag",
        "last_moved_at",
        "unit_cost_percent_rank",
        "next2_weight_kg",
        "unit_cost_percent_rank_2",
        "unit_cost_cume_dist",
        F.to_date(F.col("first_moved_at")).alias("first_moved_at_date"),
    )
    .orderBy(F.col("unit_cost_percent_rank").asc_nulls_last(), F.col("id").desc())
)
