from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_code_list_item_category = Window.partitionBy("sku_code_list_item", "category").orderBy(F.col("min_weight_kg").desc_nulls_first(), "category", "category_group", "sku_code_list_item").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_min_weight_kg_running = Window.orderBy(F.col("min_weight_kg").desc(), "category", "category_group", "sku_code_list_item").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_code_list_item_category_2 = Window.partitionBy("sku_code_list_item", "category_group")
w_category_group_by_n_rows = Window.partitionBy("category_group").orderBy(F.col("n_rows").desc(), "category", "category_group", "sku_code_list_item")

recent_skus = (
    df_skus
    .filter(~F.col("is_active"))
    .select(
        "id",
        "sku_code",
        "category",
        "unit_cost",
        "weight_kg",
        "labels",
        "is_active",
        F.when(F.col("category").isin("Beauty", "Apparel"), "group_a").when(F.col("category") == "Food", "group_b").otherwise("other").alias("category_group"),
    )
)

totals = (
    recent_skus
    .groupBy("category", "category_group")
    .agg(
        F.array_sort(F.collect_set("category")).alias("category_list"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.count("*").alias("n_rows"),
        F.min("weight_kg").alias("min_weight_kg"),
    )
    .filter(F.col("n_rows") >= 2)
)

flattened = (
    totals
    .select(
        "category",
        "category_group",
        "category_list",
        "n_rows",
        "min_weight_kg",
        F.explode("sku_code_list").alias("sku_code_list_item"),
    )
)

ranked = (
    flattened
    .select(
        "*",
        F.nth_value("min_weight_kg", 2).over(w_sku_code_list_item_category).alias("second_min_weight_kg"),
        F.max("n_rows").over(w_all_by_min_weight_kg_running).alias("max_n_rows_so_far"),
        F.round(F.col("n_rows") - F.avg("n_rows").over(w_sku_code_list_item_category_2), 2).alias("n_rows_vs_sku_code_list_item_category_group_avg"),
        F.dense_rank().over(w_category_group_by_n_rows).alias("n_rows_dense_rank"),
        F.nth_value("n_rows", 2).over(w_sku_code_list_item_category).alias("second_n_rows"),
    )
)

tagged = (
    ranked
    .select(
        "*",
        F.length(F.trim(F.col("sku_code_list_item"))).alias("sku_code_list_item_len"),
        F.upper(F.trim(F.col("category_group"))).alias("category_group_clean"),
        F.concat_ws(": ", F.col("category_group"), F.col("category")).alias("category_group_category_label"),
    )
)

result = (
    tagged
    .filter(F.col("n_rows_vs_sku_code_list_item_category_group_avg").between(-10.8, 2.4))
    .select(
        "category",
        "category_list",
        "n_rows",
        "sku_code_list_item",
        "second_min_weight_kg",
        "max_n_rows_so_far",
        "n_rows_vs_sku_code_list_item_category_group_avg",
        "n_rows_dense_rank",
        "second_n_rows",
        "category_group_clean",
        "category_group_category_label",
    )
    .orderBy(F.col("n_rows_dense_rank").desc())
)
