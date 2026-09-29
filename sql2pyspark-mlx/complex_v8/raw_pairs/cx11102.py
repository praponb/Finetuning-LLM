from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_year_distinct = Window.partitionBy(F.year("moved_at_month"), "distinct_sku_ids")
w_moved_at_month_month_distinc = Window.partitionBy(F.month("moved_at_month"), "distinct_sku_ids").orderBy("min_unit_price", "moved_at_month")
w_distinct_sku_ids_next_distin = Window.partitionBy("distinct_sku_ids", "next_distinct_sku_ids")
w_all_by_moved_at_month_year_d = Window.orderBy("moved_at_month_year_distinct_sku_ids_avg_min_uni")
w_moved_at_month_month_next_di = Window.partitionBy(F.date_trunc("month", "moved_at_month"), "next_distinct_sku_ids").orderBy("next_distinct_sku_ids", "moved_at_month").rowsBetween(-3, Window.currentRow)

filtered_moves = (
    df_stock_moves
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
    )
)

rollup_base = (
    filtered_moves
    .filter(F.col("warehouse_id").isNotNull())
    .groupBy(F.date_trunc("month", "moved_at").alias("moved_at_month"))
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
)

with_windows = (
    rollup_base
    .select(
        "moved_at_month",
        "min_unit_price",
        "distinct_sku_ids",
        "operator_list",
        F.round(F.avg("distinct_sku_ids").over(w_moved_at_month_year_distinct), 2).alias("moved_at_month_year_distinct_sku_ids_avg_distinc"),
        F.round(F.avg("min_unit_price").over(w_moved_at_month_year_distinct), 2).alias("moved_at_month_year_distinct_sku_ids_avg_min_uni"),
        F.lead("distinct_sku_ids").over(w_moved_at_month_month_distinc).alias("next_distinct_sku_ids"),
    )
)

windowed = (
    with_windows
    .select(
        "*",
        F.round(F.col("next_distinct_sku_ids") / F.nullif(F.sum("next_distinct_sku_ids").over(w_distinct_sku_ids_next_distin), F.lit(0)), 4).alias("next_distinct_sku_ids_share_of_distinct_sku_ids_"),
        F.dense_rank().over(w_all_by_moved_at_month_year_d).alias("moved_at_month_year_distinct_sku_ids_avg_min_uni_2"),
        F.max("distinct_sku_ids").over(w_moved_at_month_month_next_di).alias("distinct_sku_ids_max_4"),
    )
)

top_per_group = (
    windowed
    .filter((F.col("moved_at_month_year_distinct_sku_ids_avg_min_uni_2") == 1) & (F.col("moved_at_month_year_distinct_sku_ids_avg_min_uni") <= 275.4))
    .drop("moved_at_month_year_distinct_sku_ids_avg_min_uni_2")
)

tagged = (
    top_per_group
    .select(
        "*",
        F.coalesce(F.col("min_unit_price"), F.lit(0.0)).alias("min_unit_price_filled"),
    )
)

result = (
    tagged
    .groupingSets(
        [["next_distinct_sku_ids", "distinct_sku_ids"], ["next_distinct_sku_ids"], []],
        "next_distinct_sku_ids", "distinct_sku_ids",
    )
    .agg(
        F.min("moved_at_month_year_distinct_sku_ids_avg_distinc").alias("min_moved_at_month_year_distinct_sku_ids_avg_dis"),
        F.min("moved_at_month").alias("first_moved_at_month"),
        F.round(F.stddev("next_distinct_sku_ids_share_of_distinct_sku_ids_"), 2).alias("stddev_next_distinct_sku_ids_share_of_distinct_s"),
    )
)
