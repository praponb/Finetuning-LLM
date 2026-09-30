from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_len_by_move_type_l = Window.partitionBy("move_type_len").orderBy(F.col("move_type_len").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_len_by_move_type_l_2 = Window.partitionBy("move_type_len").orderBy(F.col("move_type_len").desc(), "id")
w_move_type_len_by_move_type_l_3 = Window.partitionBy("move_type_len").orderBy(F.col("move_type_len").desc(), "id").rowsBetween(-6, Window.currentRow)

filtered = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.abs(F.col("qty") - 191).alias("qty_distance"),
    )
)

sequenced = (
    filtered
    .select(
        "*",
        F.last("qty").over(w_move_type_len_by_move_type_l).alias("last_qty"),
        F.round(F.percent_rank().over(w_move_type_len_by_move_type_l_2), 3).alias("move_type_len_percent_rank"),
        (F.col("move_type_len") - F.lag("move_type_len").over(w_move_type_len_by_move_type_l_2)).alias("move_type_len_change"),
        F.nth_value("moved_at", 2).over(w_move_type_len_by_move_type_l).alias("second_moved_at"),
        F.max("unit_price").over(w_move_type_len_by_move_type_l_3).alias("unit_price_max_7"),
    )
)

move_type_stats = (
    sequenced
    .groupBy("move_type")
    .agg(F.avg("qty_distance").alias("avg_qty_distance"))
)

outliers = (
    sequenced.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.qty_distance") > F.col("g.avg_qty_distance"))
    .select(
        "s.*",
        "g.avg_qty_distance",
        F.round(F.col("s.qty_distance") - F.col("g.avg_qty_distance"), 2).alias("qty_distance_minus_avg"),
    )
)

filtered_2 = (
    outliers.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
    .filter(F.col("sku_id") >= 11)
)

result = (
    filtered_2
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "qty_plus_unit_price",
        "move_type_len",
        "qty_distance",
        "last_qty",
        "move_type_len_change",
        "second_moved_at",
        "unit_price_max_7",
        "avg_qty_distance",
        "qty_distance_minus_avg",
        F.substring(F.trim(F.col("move_type")), 1, 3).alias("move_type_prefix"),
    )
    .orderBy(
        F.col("avg_qty_distance").desc(),
        F.col("move_type").desc_nulls_first(),
        "id",
    )
    .limit(10)
)
