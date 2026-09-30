from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_operator = Window.partitionBy("operator")
w_warehouse_id_by_qty_bucket_f = Window.partitionBy("warehouse_id").orderBy(F.col("qty_bucket").desc_nulls_first(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_sku_weight_k = Window.partitionBy("warehouse_id").orderBy(F.col("sku_weight_kg").desc(), "id")
w_operator_move_type_by_sku_we = Window.partitionBy("operator", "move_type").orderBy(F.col("sku_weight_kg").desc(), "id").rowsBetween(-2, Window.currentRow)
w_operator_moved_at_year_by_mo = Window.partitionBy("operator", F.year("moved_at")).orderBy(F.col("moved_at").desc(), "id")
w_all_by_qty_bucket = Window.orderBy("qty_bucket", "id")
w_warehouse_id_by_sku_weight_k_2 = Window.partitionBy("warehouse_id").orderBy(F.col("sku_weight_kg").desc())
w_operator_move_type_by_sku_we_2 = Window.partitionBy("operator", "move_type").orderBy(F.col("sku_weight_kg").desc())
w_operator_move_type_by_sku_we_3 = Window.partitionBy("operator", "move_type").orderBy(F.col("sku_weight_kg").desc(), "id")

base = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (F.col("qty") > 145))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

joined = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.operator_prefix",
        "s.qty_bucket",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

sequenced = (
    joined
    .select(
        "*",
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.round(F.col("qty_bucket") - F.avg("qty_bucket").over(w_operator), 2).alias("qty_bucket_vs_operator_avg"),
        F.nth_value("moved_at", 2).over(w_warehouse_id_by_qty_bucket_f).alias("second_moved_at"),
        (F.col("qty_bucket") - F.lag("qty_bucket").over(w_warehouse_id_by_sku_weight_k)).alias("qty_bucket_change"),
        F.round(F.avg("sku_weight_kg").over(w_operator_move_type_by_sku_we), 2).alias("sku_weight_kg_avg_3"),
        F.lead("moved_at").over(w_operator_moved_at_year_by_mo).alias("next_moved_at"),
        F.round(F.cume_dist().over(w_all_by_qty_bucket), 3).alias("qty_bucket_cume_dist"),
        F.ntile(5).over(w_operator_moved_at_year_by_mo).alias("moved_at_quintile"),
        F.round(F.percent_rank().over(w_warehouse_id_by_sku_weight_k_2), 3).alias("sku_weight_kg_percent_rank"),
        F.dense_rank().over(w_operator_move_type_by_sku_we_2).alias("sku_weight_kg_dense_rank"),
        F.round(F.col("sku_weight_kg") / F.nullif(F.sum("sku_weight_kg").over(w_operator), F.lit(0)), 4).alias("sku_weight_kg_share_of_operator"),
        F.row_number().over(w_operator_move_type_by_sku_we_3).alias("rn"),
        F.max("qty_bucket").over(w_sku_id).alias("sku_id_max_qty_bucket"),
    )
)

top_rows = (
    sequenced
    .filter((F.col("sku_weight_kg_dense_rank") <= 2) & (F.col("warehouse_wh_code").isNotNull()))
    .drop("sku_weight_kg_dense_rank")
)

result = (
    top_rows
    .filter(F.col("sku_weight_kg").between(1.8, 21.0))
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "operator",
        "operator_prefix",
        "qty_bucket",
        "warehouse_wh_code",
        "rows_in_sku_id",
        "qty_bucket_vs_operator_avg",
        "second_moved_at",
        "next_moved_at",
        "moved_at_quintile",
        "sku_weight_kg_percent_rank",
        "rn",
        F.lower(F.trim(F.col("warehouse_wh_code"))).alias("warehouse_wh_code_clean"),
        F.month(F.col("next_moved_at")).alias("next_moved_at_month"),
    )
    .orderBy(
        F.col("warehouse_wh_code_clean").desc_nulls_first(),
        F.col("warehouse_id").desc_nulls_first(),
    )
)
