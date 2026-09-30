from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_warehouse_opened_on_r = Window.orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_opened_on_year = Window.partitionBy(F.year("warehouse_opened_on"))
w_all_by_days_since_moved_at_f = Window.orderBy("days_since_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_all_by_warehouse_opened_on = Window.orderBy("warehouse_opened_on", "id")
w_all_by_days_since_moved_at_t = Window.orderBy("days_since_moved_at")
w_all_by_days_since_moved_at = Window.orderBy("days_since_moved_at", "id")
w_all_by_warehouse_opened_on_y = Window.orderBy(F.col("warehouse_opened_on_year_sum_sku_weight_kg").desc())
w_prev2_warehouse_opened_on_mo = Window.partitionBy(F.date_trunc("month", "prev2_warehouse_opened_on"), F.date_trunc("month", "next_warehouse_opened_on"))
w_moved_at_year_first_moved_at = Window.partitionBy(F.year("moved_at"), F.month("first_moved_at")).orderBy("first_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_warehouse_opened_on_f = Window.orderBy(F.col("warehouse_opened_on").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_moved_at_year_first_moved_at_2 = Window.partitionBy(F.year("moved_at"), F.month("first_moved_at")).orderBy("first_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

cleaned_moves = (
    df_stock_moves
    .filter((F.col("qty") <= 372) & (F.col("warehouse_id") >= 2))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.when(F.col("qty") >= 300, "HIGH").when(F.col("qty") >= 139, "MEDIUM").otherwise("LOW").alias("qty_tier"),
    )
)

with_details = (
    cleaned_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.days_since_moved_at",
        "s.qty_tier",
        F.col("sk.labels").alias("sku_labels"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

sequenced = (
    with_details
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "days_since_moved_at",
        "qty_tier",
        "sku_labels",
        "warehouse_opened_on",
        F.first("sku_weight_kg", ignorenulls=True).over(w_all_by_warehouse_opened_on_r).alias("first_known_sku_weight_kg"),
        F.count(F.lit(1)).over(w_warehouse_opened_on_year).alias("rows_in_warehouse_opened_on_year"),
        F.sum("sku_weight_kg").over(w_warehouse_opened_on_year).alias("warehouse_opened_on_year_sum_sku_weight_kg"),
        F.first("moved_at").over(w_all_by_days_since_moved_at_f).alias("first_moved_at"),
        F.lag("warehouse_opened_on", 2).over(w_all_by_warehouse_opened_on).alias("prev2_warehouse_opened_on"),
        F.rank().over(w_all_by_days_since_moved_at_t).alias("days_since_moved_at_rank"),
        F.max("days_since_moved_at").over(w_all_by_warehouse_opened_on_r).alias("max_days_since_moved_at_so_far"),
        F.lead("warehouse_opened_on").over(w_all_by_days_since_moved_at).alias("next_warehouse_opened_on"),
        F.round(F.avg("sku_weight_kg").over(w_warehouse_opened_on_year), 2).alias("warehouse_opened_on_year_avg_sku_weight_kg"),
        F.max("sku_weight_kg").over(w_warehouse_opened_on_year).alias("warehouse_opened_on_year_max_sku_weight_kg"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at").over(w_warehouse_opened_on_year), F.lit(0)), 4).alias("days_since_moved_at_share_of_warehouse_opened_on"),
    )
)

leaders = (
    sequenced
    .filter((F.col("days_since_moved_at_rank") <= 3) & (F.month(F.col("first_moved_at")).isin(2, 3)))
)

windowed = (
    leaders
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "warehouse_opened_on",
        "warehouse_opened_on_year_sum_sku_weight_kg",
        "first_moved_at",
        "prev2_warehouse_opened_on",
        "days_since_moved_at_rank",
        "next_warehouse_opened_on",
        "warehouse_opened_on_year_avg_sku_weight_kg",
        "warehouse_opened_on_year_max_sku_weight_kg",
        F.round(F.percent_rank().over(w_all_by_warehouse_opened_on_y), 3).alias("warehouse_opened_on_year_sum_sku_weight_kg_perce"),
        F.sum("first_known_sku_weight_kg").over(w_prev2_warehouse_opened_on_mo).alias("prev2_warehouse_opened_on_month_next_warehouse_o"),
        F.last("days_since_moved_at_rank", ignorenulls=True).over(w_moved_at_year_first_moved_at).alias("last_known_days_since_moved_at_rank"),
        F.first("days_since_moved_at").over(w_all_by_warehouse_opened_on_f).alias("first_days_since_moved_at"),
        F.last("moved_at").over(w_moved_at_year_first_moved_at_2).alias("last_moved_at"),
    )
)

result = (
    windowed
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "unit_price",
        "warehouse_opened_on",
        "warehouse_opened_on_year_sum_sku_weight_kg",
        "first_moved_at",
        "days_since_moved_at_rank",
        "warehouse_opened_on_year_max_sku_weight_kg",
        "prev2_warehouse_opened_on_month_next_warehouse_o",
        "last_known_days_since_moved_at_rank",
        "first_days_since_moved_at",
        F.abs(F.col("days_since_moved_at_rank") - 6).alias("days_since_moved_at_rank_distance"),
        (F.col("days_since_moved_at_rank") + F.col("unit_price")).alias("days_since_moved_at_rank_plus_unit_price"),
    )
    .orderBy(F.col("days_since_moved_at_rank_distance").asc(), "first_moved_at")
)
