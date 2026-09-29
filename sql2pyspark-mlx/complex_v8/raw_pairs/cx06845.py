from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_sku_id_by_moved = Window.partitionBy("warehouse_id", "sku_id").orderBy("moved_at")
w_all_by_moved_at_ties = Window.orderBy(F.col("moved_at").desc())
w_warehouse_id_by_moved_at_tie = Window.partitionBy("warehouse_id").orderBy("moved_at")
w_all_by_moved_at_full = Window.orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_moved_at = Window.orderBy(F.col("moved_at").desc(), "id")
w_all_by_moved_at_ties_2 = Window.orderBy("moved_at")

valid_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 3) & (F.col("operator") != "op_dew"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
    )
)

with_details_sku = (
    valid_moves.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.sku_code").isNotNull()), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "sk.sku_code",
    )
)

ranked = (
    with_details_sku
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "sku_code",
        F.round(F.cume_dist().over(w_warehouse_id_sku_id_by_moved), 3).alias("moved_at_cume_dist"),
        F.dense_rank().over(w_all_by_moved_at_ties).alias("moved_at_dense_rank"),
        F.round(F.cume_dist().over(w_warehouse_id_by_moved_at_tie), 3).alias("moved_at_cume_dist_2"),
        F.round(F.cume_dist().over(w_all_by_moved_at_ties), 3).alias("moved_at_cume_dist_3"),
        F.nth_value("moved_at", 2).over(w_all_by_moved_at_full).alias("second_moved_at"),
        F.ntile(5).over(w_all_by_moved_at).alias("moved_at_quintile"),
        F.ntile(10).over(w_all_by_moved_at).alias("moved_at_decile"),
        F.dense_rank().over(w_all_by_moved_at_ties_2).alias("moved_at_dense_rank_2"),
        F.ntile(3).over(w_all_by_moved_at).alias("moved_at_tercile"),
    )
)

result = (
    ranked
    .select(
        "id",
        "warehouse_id",
        "moved_at",
        "moved_at_cume_dist",
        "moved_at_dense_rank",
        "moved_at_cume_dist_2",
        "moved_at_cume_dist_3",
        "second_moved_at",
        "moved_at_quintile",
        "moved_at_decile",
        F.lower(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
        F.round(F.col("moved_at_cume_dist_3") * 1.07, 2).alias("moved_at_cume_dist_3_adj"),
    )
    .orderBy(
        F.col("sku_code_clean").asc_nulls_last(),
        F.col("moved_at_quintile").desc_nulls_first(),
    )
)
