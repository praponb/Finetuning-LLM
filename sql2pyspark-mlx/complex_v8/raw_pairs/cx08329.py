from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_warehouse_opened_on = Window.orderBy("warehouse_opened_on", "id")
w_warehouse_city = Window.partitionBy("warehouse_city")
w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_moved_at_month_by_warehouse = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy("warehouse_opened_on", "id")
w_moved_at_month = Window.partitionBy(F.month("moved_at"))
w_move_type = Window.partitionBy("move_type")
w_warehouse_city_warehouse_id = Window.partitionBy("warehouse_city", "warehouse_id").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_city_warehouse_id_2 = Window.partitionBy("warehouse_city", "warehouse_id").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_sku_id_by_unit_price = Window.partitionBy("sku_id").orderBy("unit_price", "id")
w_sku_id_warehouse_opened_on_r = Window.partitionBy("sku_id", "warehouse_opened_on_rank_2").orderBy(F.col("prev3_moved_at").desc())
w_sku_id_warehouse_opened_on_r_2 = Window.partitionBy("sku_id", "warehouse_opened_on_rank_2").orderBy(F.col("prev3_moved_at").desc(), "id")
w_sku_id_warehouse_opened_on_r_3 = Window.partitionBy("sku_id", "warehouse_opened_on_rank_2").orderBy(F.col("prev3_moved_at").desc(), "id").rowsBetween(-2, Window.currentRow)
w_warehouse_city_by_warehouse = Window.partitionBy("warehouse_city").orderBy("warehouse_city_max_unit_price", "id")

valid = df_stock_moves.filter((F.col("qty") > 145) | (F.col("warehouse_id").isNotNull()))

combined_details = (
    valid.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.opened_on") < F.lit("2025-02-14").cast("date")), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.city").alias("warehouse_city"),
    )
)

ranked = (
    combined_details
    .select(
        "id",
        "sku_id",
        "qty",
        "moved_at",
        "warehouse_opened_on",
        "warehouse_city",
        F.rank().over(w_all_by_warehouse_opened_on).alias("warehouse_opened_on_rank"),
        F.max("unit_price").over(w_warehouse_city).alias("warehouse_city_max_unit_price"),
        F.lag("moved_at", 3).over(w_all_by_unit_price).alias("prev3_moved_at"),
        F.ntile(5).over(w_moved_at_month_by_warehouse).alias("warehouse_opened_on_quintile"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_moved_at_month), F.lit(0)), 4).alias("unit_price_share_of_moved_at_month"),
        F.sum("unit_price").over(w_move_type).alias("move_type_sum_unit_price"),
        F.rank().over(w_moved_at_month_by_warehouse).alias("warehouse_opened_on_rank_2"),
        F.min("qty").over(w_moved_at_month).alias("moved_at_month_min_qty"),
        F.lag("moved_at").over(w_warehouse_city_warehouse_id).alias("prev_moved_at"),
        F.max("qty").over(w_warehouse_city_warehouse_id_2).alias("max_qty_so_far"),
        F.ntile(3).over(w_warehouse_city_warehouse_id).alias("unit_price_tercile"),
        F.count(F.lit(1)).over(w_warehouse_city).alias("rows_in_warehouse_city"),
        F.lag("warehouse_opened_on").over(w_sku_id_by_unit_price).alias("prev_warehouse_opened_on"),
    )
)

with_windows = (
    ranked
    .select(
        "id",
        "qty",
        "moved_at",
        "warehouse_opened_on",
        "warehouse_city",
        "warehouse_opened_on_rank",
        "warehouse_city_max_unit_price",
        "prev3_moved_at",
        "warehouse_opened_on_quintile",
        "unit_price_share_of_moved_at_month",
        "warehouse_opened_on_rank_2",
        "moved_at_month_min_qty",
        "prev_moved_at",
        "max_qty_so_far",
        "rows_in_warehouse_city",
        "prev_warehouse_opened_on",
        F.rank().over(w_sku_id_warehouse_opened_on_r).alias("prev3_moved_at_rank"),
        (F.col("move_type_sum_unit_price") - F.lag("move_type_sum_unit_price").over(w_sku_id_warehouse_opened_on_r_2)).alias("move_type_sum_unit_price_change"),
        F.max("warehouse_opened_on_rank_2").over(w_sku_id_warehouse_opened_on_r_3).alias("warehouse_opened_on_rank_2_max_3"),
        F.row_number().over(w_warehouse_city_by_warehouse).alias("seq_no"),
    )
)

result = (
    with_windows
    .filter(F.col("unit_price_share_of_moved_at_month") < 0.9)
    .select(
        "id",
        "qty",
        "moved_at",
        "warehouse_opened_on",
        "warehouse_city_max_unit_price",
        "prev3_moved_at",
        "warehouse_opened_on_rank_2",
        "moved_at_month_min_qty",
        "prev_moved_at",
        "max_qty_so_far",
        "rows_in_warehouse_city",
        "prev3_moved_at_rank",
        "warehouse_opened_on_rank_2_max_3",
        "seq_no",
        (F.floor(F.col("warehouse_opened_on_rank") / 2) * 2).alias("warehouse_opened_on_rank_bucket"),
        F.when(F.col("warehouse_city").isin("Singapore", "Kuala Lumpur"), "group_a").when(F.col("warehouse_city") == "Bangkok", "group_b").otherwise("other").alias("warehouse_city_group"),
    )
    .orderBy(
        F.col("warehouse_city_group").desc(),
        F.col("prev3_moved_at_rank").desc(),
    )
)
