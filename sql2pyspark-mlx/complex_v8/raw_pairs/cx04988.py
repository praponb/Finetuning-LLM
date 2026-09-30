from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_operator_count_labels_c = Window.partitionBy("move_operator_count", "labels_count")

cleaned_skus = (
    df_skus
    .filter(F.col("weight_kg") >= 12.4)
    .select(
        "id",
        "category",
        "weight_kg",
        "labels",
        "is_active",
        F.length(F.trim(F.col("category"))).alias("category_len"),
        (F.col("unit_cost") + F.col("weight_kg")).alias("unit_cost_plus_weight_kg"),
        F.size(F.col("labels")).alias("labels_count"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator") != "op_ann", 1)).alias("move_operator_count"),
        F.count("*").alias("move_count"),
    )
)

with_stats = (
    cleaned_skus.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

metrics = (
    with_stats
    .select(
        "id",
        "category",
        "weight_kg",
        "labels",
        "category_len",
        "unit_cost_plus_weight_kg",
        "move_operator_count",
        "move_count",
        F.round(F.col("category_len") - F.avg("category_len").over(w_move_operator_count_labels_c), 2).alias("category_len_vs_move_operator_count_labels_count"),
        F.round(F.col("labels_count") / F.nullif(F.sum("labels_count").over(w_move_operator_count_labels_c), F.lit(0)), 4).alias("labels_count_share_of_move_operator_count_labels"),
        F.size(F.collect_set("category").over(w_move_operator_count_labels_c)).alias("distinct_category_in_move_operator_count_labels_"),
    )
)

qualified = (
    metrics
    .crossJoin(metrics.agg(F.max("labels_count_share_of_move_operator_count_labels").alias("overall_max_labels_count_share_of_move_operator_")))
    .filter(F.col("labels_count_share_of_move_operator_count_labels") < F.col("overall_max_labels_count_share_of_move_operator_"))
    .drop("overall_max_labels_count_share_of_move_operator_")
)

stats = (
    qualified
    .groupBy("category", "move_count")
    .agg(
        F.round(F.sum("labels_count_share_of_move_operator_count_labels") / F.nullif(F.sum("move_count"), F.lit(0)), 3).alias("labels_count_share_of_move_operator_count_labels_2"),
        F.sum(F.when(F.col("category").isin("Beauty", "Toys", "Electronics"), F.col("unit_cost_plus_weight_kg")).otherwise(0)).alias("unit_cost_plus_weight_kg_category_in_beauty_toys"),
    )
)

result = (
    stats
    .select(
        "category",
        "move_count",
        "unit_cost_plus_weight_kg_category_in_beauty_toys",
        "labels_count_share_of_move_operator_count_labels_2",
    )
    .unpivot(
        ids=["category", "move_count"],
        values=["unit_cost_plus_weight_kg_category_in_beauty_toys", "labels_count_share_of_move_operator_count_labels_2"],
        variableColumnName="metric_name",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("category", "move_count", "metric_name")
)
