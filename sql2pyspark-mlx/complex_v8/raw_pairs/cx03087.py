from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_city_moved_at_year = Window.partitionBy("warehouse_city", F.year("moved_at")).orderBy("warehouse_capacity", "id").rowsBetween(-1, -1)
w_warehouse_city_moved_at_year_2 = Window.partitionBy("warehouse_city", F.year("moved_at")).orderBy("warehouse_capacity", "id").rowsBetween(-3, -1)
w_warehouse_city_moved_at_year_3 = Window.partitionBy("warehouse_city", F.year("moved_at")).orderBy("warehouse_capacity", "id")
w_warehouse_opened_on_month_by = Window.partitionBy(F.month("warehouse_opened_on")).orderBy(F.col("unit_price").asc_nulls_last())
w_warehouse_city = Window.partitionBy("warehouse_city")
w_warehouse_opened_on_month_by_2 = Window.partitionBy(F.date_trunc("month", "warehouse_opened_on")).orderBy(F.col("moved_at").desc(), "id")
w_warehouse_city_warehouse_id = Window.partitionBy("warehouse_city", "warehouse_id")
w_all_by_warehouse_opened_on_r = Window.orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_opened_on_month_by_3 = Window.partitionBy(F.month("warehouse_opened_on")).orderBy(F.col("unit_price").asc_nulls_last(), "id")
w_all_by_warehouse_opened_on = Window.orderBy("warehouse_opened_on", "id")

recent_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id") >= 5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

enriched = (
    recent_moves.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.max("warehouse_capacity").over(w_warehouse_city_moved_at_year).alias("warehouse_capacity_max_1_prior"),
        F.max("unit_price").over(w_warehouse_city_moved_at_year_2).alias("unit_price_max_3_prior"),
        F.datediff(F.col("warehouse_opened_on"), F.lag("warehouse_opened_on").over(w_warehouse_city_moved_at_year_3)).alias("days_since_prev_warehouse_opened_on"),
        F.rank().over(w_warehouse_opened_on_month_by).alias("unit_price_rank"),
        F.min("warehouse_capacity").over(w_warehouse_city).alias("warehouse_city_min_warehouse_capacity"),
        F.lead("warehouse_opened_on", 2).over(w_warehouse_opened_on_month_by_2).alias("next2_warehouse_opened_on"),
        F.round(F.avg("warehouse_capacity").over(w_warehouse_city_moved_at_year_2), 2).alias("warehouse_capacity_avg_3_prior"),
        F.round(F.col("warehouse_capacity") / F.nullif(F.sum("warehouse_capacity").over(w_warehouse_city_warehouse_id), F.lit(0)), 4).alias("warehouse_capacity_share_of_warehouse_city_wareh"),
        F.last("warehouse_opened_on", ignorenulls=True).over(w_all_by_warehouse_opened_on_r).alias("last_known_warehouse_opened_on"),
        F.rank().over(w_warehouse_opened_on_month_by_3).alias("unit_price_rank_2"),
        F.ntile(4).over(w_warehouse_opened_on_month_by_2).alias("moved_at_quartile"),
        F.sum("warehouse_capacity").over(w_warehouse_city_warehouse_id).alias("warehouse_city_warehouse_id_sum_warehouse_capaci"),
        F.min("warehouse_capacity").over(w_warehouse_city_warehouse_id).alias("warehouse_city_warehouse_id_min_warehouse_capaci"),
        F.lead("unit_price").over(w_all_by_warehouse_opened_on).alias("next_unit_price"),
        F.round((F.unix_timestamp("moved_at") - F.unix_timestamp(F.lag("moved_at").over(w_warehouse_opened_on_month_by_2))) / 60, 1).alias("minutes_since_prev_moved_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "unit_price",
        "warehouse_city",
        "warehouse_capacity",
        "warehouse_capacity_max_1_prior",
        "unit_price_max_3_prior",
        "days_since_prev_warehouse_opened_on",
        "warehouse_city_min_warehouse_capacity",
        "next2_warehouse_opened_on",
        "warehouse_capacity_share_of_warehouse_city_wareh",
        "last_known_warehouse_opened_on",
        "unit_price_rank_2",
        "warehouse_city_warehouse_id_sum_warehouse_capaci",
        "next_unit_price",
        "minutes_since_prev_moved_at",
        F.coalesce(F.col("warehouse_capacity"), F.lit(0)).alias("warehouse_capacity_filled"),
    )
    .orderBy("warehouse_capacity_max_1_prior", F.col("moved_at").asc_nulls_last())
)
