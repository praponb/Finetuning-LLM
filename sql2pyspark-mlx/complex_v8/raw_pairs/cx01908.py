from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_unit_price_tier = Window.partitionBy("warehouse_id", "unit_price_tier")
w_move_type_len_unit_price_tie = Window.partitionBy("move_type_len", "unit_price_tier").orderBy("move_type_len", "id")

filtered = (
    df_stock_moves
    .filter(F.col("move_type") == "ADJUST")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
    )
)

tagged = (
    filtered
    .filter(F.month(F.col("moved_at")).isin(1, 2))
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.when(F.col("unit_price") >= 330.0, "large").when(F.col("unit_price") >= 160.8, "medium").otherwise("small").alias("unit_price_tier"),
    )
)

ranked = (
    tagged
    .select(
        "id",
        "warehouse_id",
        "move_type_slug",
        "move_type_len",
        F.round(F.col("move_type_len") / F.nullif(F.sum("move_type_len").over(w_warehouse_id_unit_price_tier), F.lit(0)), 4).alias("move_type_len_share_of_warehouse_id_unit_price_t"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_warehouse_id_unit_price_tier), F.lit(0)), 4).alias("unit_price_share_of_warehouse_id_unit_price_tier"),
        F.ntile(10).over(w_move_type_len_unit_price_tie).alias("move_type_len_decile"),
    )
)

derived = (
    ranked
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "move_type_slug",
        "move_type_len",
        "move_type_len_decile",
        (F.floor(F.col("move_type_len") / 2) * 2).alias("move_type_len_bucket"),
        F.abs(F.col("move_type_len") - 8).alias("move_type_len_distance"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type_slug")), " ", "_")).alias("move_type_slug_slug"),
    )
)

warehouse_id_stats = (
    derived
    .groupBy("warehouse_id")
    .agg(F.avg("move_type_len_distance").alias("avg_move_type_len_distance"))
)

outliers = (
    derived.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.move_type_len_distance") > F.col("g.avg_move_type_len_distance"))
    .select(
        "s.*",
        "g.avg_move_type_len_distance",
        F.round(F.col("s.move_type_len_distance") - F.col("g.avg_move_type_len_distance"), 2).alias("move_type_len_distance_minus_avg"),
    )
)

result = (
    outliers
    .filter(F.col("move_type_len_bucket") < 10)
    .select(
        "id",
        "warehouse_id",
        "move_type_slug",
        "move_type_len",
        "move_type_len_decile",
        "move_type_len_bucket",
        "move_type_slug_slug",
        "avg_move_type_len_distance",
        "move_type_len_distance_minus_avg",
    )
)
