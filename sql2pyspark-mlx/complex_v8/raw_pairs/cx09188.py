from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_all_by_days_since_moved_at_t = Window.orderBy(F.col("days_since_moved_at").desc())
w_operator_len = Window.partitionBy("operator_len")

base = (
    df_stock_moves
    .filter(F.col("unit_price") < 341.5)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.datediff(F.lit("2026-01-01").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
    )
)

with_trends = (
    base
    .select(
        "*",
        F.row_number().over(w_all_by_unit_price).alias("unit_price_seq"),
        F.dense_rank().over(w_all_by_days_since_moved_at_t).alias("days_since_moved_at_dense_rank"),
        F.size(F.collect_set("move_type").over(w_operator_len)).alias("distinct_move_type_in_operator_len"),
        (F.col("unit_price") - F.lag("unit_price").over(w_all_by_unit_price)).alias("unit_price_change"),
    )
)

with_details_sku = (
    with_trends.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.moved_at_hour",
        "s.operator_len",
        "s.days_since_moved_at",
        "s.unit_price_seq",
        "s.days_since_moved_at_dense_rank",
        "s.distinct_move_type_in_operator_len",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.unit_cost").alias("sku_unit_cost"),
    )
)

exploded_detail = (
    with_details_sku.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price_seq",
        "s.distinct_move_type_in_operator_len",
        "s.sku_unit_cost",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

enriched = (
    exploded_detail
    .select(
        "id",
        "move_type",
        "moved_at",
        "unit_price_seq",
        "distinct_move_type_in_operator_len",
        "move_id",
        "move_moved_at",
        F.round(F.col("sku_unit_cost") / F.nullif(F.col("unit_price_seq"), F.lit(0)), 3).alias("sku_unit_cost_per_unit_price_seq"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

with_activity = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.id") == F.col("s.move_id")) & (F.col("st.unit_price") >= 215.4) & (F.col("st.qty") < 300), "left_semi")
)

enriched_move = (
    with_activity.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.id") == F.col("s.move_id"), "inner")
    .select(
        "s.*",
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

result = (
    enriched_move
    .select("id", "move_type")
    .distinct()
    .orderBy(F.col("move_type").asc_nulls_last(), F.col("id").asc())
)
