from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_weight_kg = Window.orderBy("weight_kg", "id")
w_all_by_weight_kg_ties = Window.orderBy("weight_kg")
w_all_by_weight_kg_prior_6 = Window.orderBy("weight_kg", "id").rowsBetween(-6, -1)
w_category_first_moved_at_to_d = Window.partitionBy("category", F.to_date("first_moved_at"))

scoped_skus = (
    df_skus
    .filter(F.col("sku_code").isNotNull())
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        "labels",
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count(F.when(F.col("operator") == "op_eve", 1)).alias("move_operator_count"),
        F.count("*").alias("move_count"),
        F.min("moved_at").alias("first_moved_at"),
    )
)

with_counts = (
    scoped_skus.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        "st.first_moved_at",
    )
)

labeled = (
    with_counts
    .select(
        "*",
        F.when(F.col("category").isin("Toys", "Apparel"), "group_a").when(F.col("category") == "Beauty", "group_b").otherwise("other").alias("category_group"),
        F.when(F.col("move_operator_count") >= 4, "HIGH").when(F.col("move_operator_count") >= 2, "MEDIUM").otherwise("LOW").alias("move_operator_count_tier"),
        (F.col("weight_kg") - F.col("move_operator_count")).alias("weight_kg_minus_move_operator_count"),
    )
)

sequenced = (
    labeled
    .select(
        "id",
        "weight_kg",
        "first_moved_at",
        "category_group",
        "move_operator_count_tier",
        F.dense_rank().over(w_all_by_weight_kg).alias("weight_kg_dense_rank"),
        F.round(F.percent_rank().over(w_all_by_weight_kg_ties), 3).alias("weight_kg_percent_rank"),
        F.max("weight_kg").over(w_all_by_weight_kg_prior_6).alias("weight_kg_max_6_prior"),
        F.round(F.col("move_operator_count") / F.nullif(F.sum("move_operator_count").over(w_category_first_moved_at_to_d), F.lit(0)), 4).alias("move_operator_count_share_of_category_first_move"),
    )
)

result = (
    sequenced
    .filter(F.col("move_operator_count_share_of_category_first_move") < 0.8)
    .select(
        "id",
        "weight_kg",
        "first_moved_at",
        "category_group",
        "weight_kg_dense_rank",
        "weight_kg_percent_rank",
        "weight_kg_max_6_prior",
        F.round(F.col("weight_kg") / F.nullif(F.col("weight_kg_max_6_prior"), F.lit(0)), 3).alias("weight_kg_per_weight_kg_max_6_prior"),
    )
    .orderBy(F.col("weight_kg_per_weight_kg_max_6_prior").desc_nulls_first())
)
