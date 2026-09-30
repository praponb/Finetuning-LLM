from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("warehouse_id", "sku_id").orderBy("qty", "id")
w_last_moved_at_month_by_wareh = Window.partitionBy(F.month("last_moved_at")).orderBy("warehouse_opened_on", "id")
w_move_type_operator = Window.partitionBy("move_type", "operator")
w_last_moved_at_month_by_wareh_2 = Window.partitionBy(F.month("last_moved_at")).orderBy("warehouse_opened_on", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)

recent = (
    df_stock_moves
    .filter((F.col("qty") >= 102) & (F.col("moved_at") < F.lit("2025-03-08 11:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") * 1.1, 2).alias("unit_price_adj"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

latest_per_key = (
    recent
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.min("moved_at").alias("first_moved_at"),
        F.round(F.avg("qty"), 2).alias("avg_move_qty"),
    )
)

with_counts = (
    latest_per_key.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        "st.first_moved_at",
        "st.avg_move_qty",
    )
)

with_parent_warehouse = (
    with_counts.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .filter(F.col("w.opened_on") < F.lit("2025-07-03").cast("date"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price_adj",
        "s.days_since_moved_at",
        "s.operator_clean",
        "s.last_moved_at",
        "s.first_moved_at",
        "s.avg_move_qty",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.category").alias("sku_category"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

filtered = (
    with_parent_warehouse.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") < 293.2), "left_semi")
)

enriched = (
    filtered
    .filter(F.col("unit_price_adj") <= 637.6)
    .select(
        "id",
        "move_type",
        "operator",
        "unit_price_adj",
        "days_since_moved_at",
        "operator_clean",
        "last_moved_at",
        "avg_move_qty",
        "warehouse_opened_on",
        "sku_labels",
        F.round((F.unix_timestamp(F.col("last_moved_at")) - F.unix_timestamp(F.col("first_moved_at"))) / 60, 1).alias("minutes_first_moved_at_to_last_moved_at"),
    )
)

windowed = (
    enriched
    .select(
        "id",
        "move_type",
        "days_since_moved_at",
        "operator_clean",
        "last_moved_at",
        "avg_move_qty",
        "sku_labels",
        "minutes_first_moved_at_to_last_moved_at",
        F.dense_rank().over(w_last_moved_at_month_by_wareh).alias("warehouse_opened_on_dense_rank"),
        F.round(F.col("days_since_moved_at") / F.nullif(F.sum("days_since_moved_at").over(w_move_type_operator), F.lit(0)), 4).alias("days_since_moved_at_share_of_move_type_operator"),
        F.nth_value("warehouse_opened_on", 2).over(w_last_moved_at_month_by_wareh_2).alias("second_warehouse_opened_on"),
        F.row_number().over(w_last_moved_at_month_by_wareh).alias("seq_no"),
    )
)

result = (
    windowed
    .groupBy("warehouse_opened_on_dense_rank", "second_warehouse_opened_on")
    .pivot("move_type", ["INBOUND", "TRANSFER"])
    .agg(F.avg("days_since_moved_at_share_of_move_type_operator"))
    .orderBy("warehouse_opened_on_dense_rank", "second_warehouse_opened_on")
)
