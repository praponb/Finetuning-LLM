from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_category_distinct_sku_ca = Window.partitionBy("sku_category", "distinct_sku_categorys").orderBy("distinct_warehouse_ids", "sku_category", "operator").rowsBetween(-2, -1)
w_operator = Window.partitionBy("operator")

filtered_moves = (
    df_stock_moves
    .filter(F.col("unit_price") >= 124.1)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
    )
)

joined_sku = (
    filtered_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "sk.sku_code",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.category").alias("sku_category"),
    )
)

per_group = (
    joined_sku
    .groupBy("sku_category", "operator")
    .agg(
        F.count("*").alias("n_rows"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.array_sort(F.collect_set("sku_category")).alias("sku_category_list"),
        F.countDistinct("warehouse_id").alias("distinct_warehouse_ids"),
        F.countDistinct("sku_category").alias("distinct_sku_categorys"),
    )
    .filter(F.col("n_rows") >= 2)
)

with_windows = (
    per_group
    .select(
        "sku_category",
        "operator",
        "n_rows",
        "sku_code_list",
        "distinct_warehouse_ids",
        "distinct_sku_categorys",
        F.round(F.avg("distinct_sku_categorys").over(w_sku_category_distinct_sku_ca), 2).alias("distinct_sku_categorys_avg_2_prior"),
        F.count(F.lit(1)).over(w_operator).alias("rows_in_operator"),
    )
)

result = (
    with_windows
    .groupBy("n_rows", "distinct_sku_categorys")
    .pivot("operator", ["op_ann", "op_chan", "op_bee", "op_eve"])
    .agg(F.count("distinct_sku_categorys_avg_2_prior"))
    .orderBy("n_rows", "distinct_sku_categorys")
)
