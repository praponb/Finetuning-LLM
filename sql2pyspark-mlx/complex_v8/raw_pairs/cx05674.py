from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_weight_kg_plus_unit_c = Window.orderBy(F.col("weight_kg_plus_unit_cost").desc_nulls_first())
w_category_group_by_weight_kg = Window.partitionBy("category_group").orderBy(F.col("weight_kg").desc())
w_all_by_weight_kg_plus_unit_c_2 = Window.orderBy(F.col("weight_kg_plus_unit_cost").desc_nulls_first(), "id")
w_category_group = Window.partitionBy("category_group")
w_all_by_weight_kg_plus_unit_c_3 = Window.orderBy(F.col("weight_kg_plus_unit_cost").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_rn_distinct_category_in_cate = Window.partitionBy("rn", "distinct_category_in_category_group")
w_category_group_by_distinct_c = Window.partitionBy("category_group").orderBy("distinct_category_in_category_group", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_category_group_by_distinct_c_2 = Window.partitionBy("category_group").orderBy("distinct_category_in_category_group")

base = (
    df_skus
    .select(
        "id",
        "category",
        "weight_kg",
        "labels",
        F.when(F.col("category").isin("Apparel", "Food"), "group_a").when(F.col("category") == "Toys", "group_b").otherwise("other").alias("category_group"),
        (F.col("weight_kg") + F.col("unit_cost")).alias("weight_kg_plus_unit_cost"),
        F.round(F.col("unit_cost") * 1.1, 2).alias("unit_cost_adj"),
    )
)

windowed = (
    base
    .select(
        "*",
        F.rank().over(w_all_by_weight_kg_plus_unit_c).alias("weight_kg_plus_unit_cost_rank"),
        F.round(F.cume_dist().over(w_category_group_by_weight_kg), 3).alias("weight_kg_cume_dist"),
        F.row_number().over(w_all_by_weight_kg_plus_unit_c_2).alias("rn"),
        F.size(F.collect_set("category").over(w_category_group)).alias("distinct_category_in_category_group"),
        F.max("weight_kg").over(w_category_group).alias("category_group_max_weight_kg"),
        F.nth_value("unit_cost_adj", 2).over(w_all_by_weight_kg_plus_unit_c_3).alias("second_unit_cost_adj"),
    )
)

without_matches = (
    windowed.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.id"), "left_semi")
    .filter(F.col("category_group_max_weight_kg") >= 50.1)
)

metrics = (
    without_matches
    .select(
        "*",
        F.max("unit_cost_adj").over(w_rn_distinct_category_in_cate).alias("rn_distinct_category_in_category_group_max_unit_"),
        F.first("weight_kg_plus_unit_cost").over(w_category_group_by_distinct_c).alias("first_weight_kg_plus_unit_cost"),
        F.nth_value("second_unit_cost_adj", 2).over(w_category_group_by_distinct_c).alias("second_second_unit_cost_adj"),
        F.round(F.col("unit_cost_adj") - F.avg("unit_cost_adj").over(w_rn_distinct_category_in_cate), 2).alias("unit_cost_adj_vs_rn_distinct_category_in_categor"),
        F.round(F.cume_dist().over(w_category_group_by_distinct_c_2), 3).alias("distinct_category_in_category_group_cume_dist"),
    )
)

derived = (
    metrics
    .filter(F.array_contains(F.col("labels"), "cold"))
    .select(
        "id",
        "category",
        "labels",
        "category_group",
        "weight_kg_plus_unit_cost_rank",
        "weight_kg_cume_dist",
        "rn",
        "distinct_category_in_category_group",
        "category_group_max_weight_kg",
        "second_unit_cost_adj",
        "rn_distinct_category_in_category_group_max_unit_",
        "first_weight_kg_plus_unit_cost",
        "second_second_unit_cost_adj",
        "unit_cost_adj_vs_rn_distinct_category_in_categor",
        "distinct_category_in_category_group_cume_dist",
        F.concat(F.col("category"), F.lit("#"), F.col("rn").cast("string")).alias("category_ref"),
        F.abs(F.col("weight_kg_cume_dist") - 0.5).alias("weight_kg_cume_dist_distance"),
        F.upper(F.trim(F.col("category_group"))).alias("category_group_clean"),
    )
)

filtered = (
    derived.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.id")) & (F.hour(F.col("st.moved_at")).between(10, 15)), "left_semi")
)

scored = (
    filtered
    .filter(F.col("rn") < 9)
    .withColumn("category_group_2", F.when(F.col("category").isin("Apparel", "Food"), "group_a").when(F.col("category") == "Toys", "group_b").otherwise("other"))
)

result = (
    scored
    .select(
        "id",
        "labels",
        "category_group",
        "weight_kg_plus_unit_cost_rank",
        "weight_kg_cume_dist",
        "rn",
        "distinct_category_in_category_group",
        "category_group_max_weight_kg",
        "second_unit_cost_adj",
        "rn_distinct_category_in_category_group_max_unit_",
        "first_weight_kg_plus_unit_cost",
        "unit_cost_adj_vs_rn_distinct_category_in_categor",
        "distinct_category_in_category_group_cume_dist",
        "weight_kg_cume_dist_distance",
        "category_group_clean",
        "category_group_2",
        F.lower(F.regexp_replace(F.trim(F.col("category_group_2")), " ", "_")).alias("category_group_2_slug"),
    )
    .orderBy(F.col("category_group").desc_nulls_first(), F.col("id").desc())
)
