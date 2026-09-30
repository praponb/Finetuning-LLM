from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_warehouse_id_by_un = Window.partitionBy("move_type", "warehouse_id").orderBy(F.col("unit_price").asc_nulls_last(), "id")
w_operator_warehouse_id_by_qty = Window.partitionBy("operator", "warehouse_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_month_move_type = Window.partitionBy(F.date_trunc("month", "moved_at"), "move_type")
w_moved_at_month_move_type_by = Window.partitionBy(F.month("moved_at"), "move_type").orderBy(F.col("moved_at").asc_nulls_last(), "id")

filtered = (
    df_stock_moves
    .filter(F.col("unit_price") > 206.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

windowed = (
    filtered
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.round(F.percent_rank().over(w_move_type_warehouse_id_by_un), 3).alias("unit_price_percent_rank"),
        F.first("qty", ignorenulls=True).over(w_operator_warehouse_id_by_qty).alias("first_known_qty"),
    )
)

joined_sku = (
    windowed.alias("s")
    .join(df_skus.alias("sk"), (F.col("sk.id") == F.col("s.sku_id")) & (F.col("sk.sku_code").isNotNull()), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price_percent_rank",
        F.col("sk.is_active").alias("sku_is_active"),
    )
)

tagged = (
    joined_sku
    .select(
        "id",
        "move_type",
        "moved_at",
        "unit_price_percent_rank",
        F.concat(F.col("move_type"), F.lit("#"), F.col("id").cast("string")).alias("move_type_ref"),
        F.abs(F.col("unit_price_percent_rank") - 0.5).alias("unit_price_percent_rank_distance"),
        F.round(F.col("unit_price_percent_rank") * 0.9, 2).alias("unit_price_percent_rank_adj"),
    )
)

tagged_2 = (
    tagged
    .filter(F.col("move_type") == "INBOUND")
    .select(
        "*",
        F.concat(F.col("move_type_ref"), F.lit("#"), F.col("id").cast("string")).alias("move_type_ref_ref"),
        F.upper(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

metrics = (
    tagged_2
    .select(
        "id",
        "move_type",
        "unit_price_percent_rank_distance",
        "unit_price_percent_rank_adj",
        "move_type_clean",
        F.min("unit_price_percent_rank").over(w_moved_at_month_move_type).alias("moved_at_month_move_type_min_unit_price_percent_"),
        F.lag("moved_at").over(w_moved_at_month_move_type_by).alias("prev_moved_at"),
    )
)

result = (
    metrics
    .select(
        "id",
        "unit_price_percent_rank_distance",
        "unit_price_percent_rank_adj",
        "move_type_clean",
        "moved_at_month_move_type_min_unit_price_percent_",
        F.lower(F.regexp_replace(F.trim(F.col("move_type_clean")), " ", "_")).alias("move_type_clean_slug"),
    )
    .orderBy(F.col("id").desc(), F.col("move_type_clean_slug").desc())
)
