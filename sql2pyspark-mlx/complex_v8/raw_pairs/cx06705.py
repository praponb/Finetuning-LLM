from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("qty_distance").desc(), F.col("id").desc())
w_all_by_qty_distance_ties = Window.orderBy("qty_distance")
w_all_by_qty_distance = Window.orderBy("qty_distance", "id")
w_all_by_qty_distance_running = Window.orderBy("qty_distance", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_qty_distance_full = Window.orderBy("qty_distance", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_group = Window.partitionBy("move_type_group")

cleaned_moves = (
    df_stock_moves
    .filter(F.col("operator").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.abs(F.col("qty") - 191).alias("qty_distance"),
        F.when(F.col("move_type").isin("TRANSFER", "OUTBOUND"), "group_a").when(F.col("move_type") == "ADJUST", "group_b").otherwise("other").alias("move_type_group"),
        F.initcap(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

deduped = (
    cleaned_moves
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_windows = (
    deduped
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        "qty_distance",
        "move_type_group",
        "operator_clean",
        F.round(F.cume_dist().over(w_all_by_qty_distance_ties), 3).alias("qty_distance_cume_dist"),
        (F.col("qty") - F.lag("qty").over(w_all_by_qty_distance)).alias("qty_change"),
        F.max("qty").over(w_all_by_qty_distance_running).alias("max_qty_so_far"),
        F.last("qty").over(w_all_by_qty_distance_full).alias("last_qty"),
        F.sum("unit_price").over(w_move_type_group).alias("move_type_group_sum_unit_price"),
    )
)

combined_details_sku = (
    with_windows.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.length(F.trim(F.col("sk.sku_code"))) >= 7)
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.operator",
        "s.qty_distance",
        "s.operator_clean",
        "s.qty_change",
        "s.last_qty",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        "sk.sku_code",
    )
)

result = (
    combined_details_sku
    .cube("warehouse_id", "sku_id")
    .agg(
        F.grouping_id().alias("grouping_level"),
        F.array_sort(F.collect_set("sku_code")).alias("sku_code_list"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
