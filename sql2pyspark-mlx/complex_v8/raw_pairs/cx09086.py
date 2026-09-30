from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_is_active_label_n_category_t = Window.partitionBy("is_active_label", "n_category_toys").orderBy("n_category_toys_plus_avg_weight_kg", "category", "is_active_label").rowsBetween(-6, Window.currentRow)
w_is_active_label_n_category_t_2 = Window.partitionBy("is_active_label", "n_category_toys").orderBy("n_category_toys_plus_avg_weight_kg", "category", "is_active_label")

prepared_skus = (
    df_skus
    .filter(F.size(F.col("labels")) >= 1)
    .select(
        "id",
        "sku_code",
        "category",
        "weight_kg",
        (F.col("unit_cost") - F.col("weight_kg")).alias("unit_cost_minus_weight_kg"),
        F.concat_ws(" / ", F.col("category"), F.col("sku_code")).alias("category_sku_code_label"),
        F.when(F.col("is_active"), "yes").otherwise("no").alias("is_active_label"),
    )
)

qualified = (
    prepared_skus
    .crossJoin(prepared_skus.agg(F.avg("weight_kg").alias("overall_avg_weight_kg")))
    .filter(F.col("weight_kg") > F.col("overall_avg_weight_kg"))
    .drop("overall_avg_weight_kg")
)

rollup_base = (
    qualified
    .groupBy("category", "is_active_label")
    .agg(
        F.round(F.avg("weight_kg"), 2).alias("avg_weight_kg"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.sum(F.when(F.col("category") == "Toys", 1).otherwise(0)).alias("n_category_toys"),
        F.avg("unit_cost_minus_weight_kg").alias("avg_unit_cost_minus_weight_kg_check"),
    )
    .filter(F.col("avg_unit_cost_minus_weight_kg_check") > 85.1)
    .drop("avg_unit_cost_minus_weight_kg_check")
)

scored = (
    rollup_base
    .filter(F.col("is_active_label").isNotNull())
    .select(
        "category",
        "is_active_label",
        "n_category_toys",
        F.length(F.trim(F.col("category"))).alias("category_len"),
        (F.col("n_category_toys") + F.col("avg_weight_kg")).alias("n_category_toys_plus_avg_weight_kg"),
        F.length(F.trim(F.col("category"))).alias("category_len_2"),
    )
)

sequenced = (
    scored
    .select(
        "*",
        F.max("category_len_2").over(w_is_active_label_n_category_t).alias("category_len_2_max_7"),
        F.round(F.cume_dist().over(w_is_active_label_n_category_t_2), 3).alias("n_category_toys_plus_avg_weight_kg_cume_dist"),
        (F.col("category_len_2") - F.lag("category_len_2").over(w_is_active_label_n_category_t_2)).alias("category_len_2_change"),
        F.row_number().over(w_is_active_label_n_category_t_2).alias("seq_no"),
    )
)

result = (
    sequenced
    .rollup("category_len", "seq_no", "category_len_2")
    .agg(
        F.countDistinct("is_active_label").alias("distinct_is_active_labels"),
        F.countDistinct("category").alias("distinct_categorys"),
    )
    .orderBy(
        F.col("category_len").asc_nulls_last(),
        F.col("seq_no").asc_nulls_last(),
        F.col("category_len_2").asc_nulls_last(),
    )
)
