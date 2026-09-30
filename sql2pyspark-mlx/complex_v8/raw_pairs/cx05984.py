from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_move_type_by_un = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("unit_price_bucket").desc_nulls_first(), "id")
w_all_by_moved_at_ties = Window.orderBy("moved_at")
w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_sku_id = Window.partitionBy("sku_id")

cleaned_moves = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-02-12").cast("date")) & (F.col("unit_price") <= 367.6))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
    )
)

without_matches = (
    cleaned_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
    .filter(F.col("unit_price") > 99.1)
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

summarized_rows = (
    without_matches.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

windowed = (
    summarized_rows
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_bucket",
        "move_count",
        F.lag("unit_price_adj").over(w_warehouse_id_move_type_by_un).alias("prev_unit_price_adj"),
        F.round(F.percent_rank().over(w_all_by_moved_at_ties), 3).alias("moved_at_percent_rank"),
        F.lead("unit_price_adj", 2, 0).over(w_all_by_moved_at).alias("next2_unit_price_adj"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
    )
)

filtered = (
    windowed.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.opened_on") < F.lit("2025-07-03").cast("date")), "left_semi")
)

result = (
    filtered
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_bucket",
        "move_count",
        "moved_at_percent_rank",
        "next2_unit_price_adj",
        "rows_in_sku_id",
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)
