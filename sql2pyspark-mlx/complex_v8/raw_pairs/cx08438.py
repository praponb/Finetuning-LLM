from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_by_unit_price = Window.partitionBy(F.date_trunc("month", "moved_at")).orderBy(F.col("unit_price").desc())
w_all_by_qty_moving_3 = Window.orderBy(F.col("qty").desc_nulls_first(), "id").rowsBetween(-3, Window.currentRow)
w_moved_at_month = Window.partitionBy(F.date_trunc("month", "moved_at"))
w_sku_id = Window.partitionBy("sku_id")
w_move_type_by_total_unit_pric = Window.partitionBy("move_type").orderBy("total_unit_price", "move_type", "sku_id").rowsBetween(-6, -1)

prepared = (
    df_stock_moves
    .filter((F.col("move_type") != "ADJUST") & (F.col("sku_id") >= 16))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
    )
)

windowed = (
    prepared
    .select(
        "*",
        F.round(F.cume_dist().over(w_moved_at_month_by_unit_price), 3).alias("unit_price_cume_dist"),
        F.max("unit_price").over(w_all_by_qty_moving_3).alias("unit_price_max_4"),
        F.count(F.lit(1)).over(w_moved_at_month).alias("rows_in_moved_at_month"),
    )
)

filtered = (
    windowed.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator") == "op_chan"), "left_anti")
)

rollup_base = (
    filtered
    .groupBy("move_type", "sku_id")
    .agg(
        F.min("qty_bucket").alias("min_qty_bucket"),
        F.round(F.sum("unit_price"), 2).alias("total_unit_price"),
        F.percentile_approx("unit_price_max_4", 0.5).alias("median_unit_price_max_4"),
    )
)

with_trends = (
    rollup_base
    .select(
        "move_type",
        "sku_id",
        "min_qty_bucket",
        "total_unit_price",
        "median_unit_price_max_4",
        F.round(F.avg("min_qty_bucket").over(w_sku_id), 2).alias("sku_id_avg_min_qty_bucket"),
        F.round(F.avg("median_unit_price_max_4").over(w_move_type_by_total_unit_pric), 2).alias("median_unit_price_max_4_avg_6_prior"),
    )
)

combined_details = (
    with_trends.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.*",
        "sk.sku_code",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

result = (
    combined_details
    .rollup("sku_id", "sku_category")
    .agg(
        F.grouping("sku_id").alias("is_total_sku_id"),
        F.countDistinct("sku_id").alias("distinct_sku_ids"),
    )
    .select(
        "sku_id",
        F.coalesce(F.col("sku_category"), F.lit("ALL")).alias("sku_category"),
        "is_total_sku_id",
        "distinct_sku_ids",
    )
)
