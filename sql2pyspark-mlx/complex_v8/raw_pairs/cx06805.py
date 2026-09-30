from pyspark.sql import functions as F

base_moves = (
    df_stock_moves
    .filter(F.col("unit_price") <= 299.1)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.substring(F.trim(F.col("move_type")), 1, 2).alias("move_type_prefix"),
        F.round(F.col("unit_price") * 0.25, 2).alias("unit_price_adj"),
    )
)

with_activity = (
    base_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator") != "op_ann") & (F.hour(F.col("st.moved_at")).between(10, 15)), "left_anti")
)

tagged = (
    with_activity
    .filter(F.col("move_type_prefix").isNotNull())
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "move_type_prefix",
        (F.floor(F.col("unit_price_adj") / 100) * 100).alias("unit_price_adj_bucket"),
        (F.floor(F.col("unit_price_adj") / 100) * 100).alias("unit_price_adj_bucket_2"),
    )
)

derived = (
    tagged
    .select(
        "*",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
        (F.floor(F.col("unit_price_adj_bucket") / 100) * 100).alias("unit_price_adj_bucket_bucket"),
    )
)

per_group = (
    derived
    .groupBy(
        "sku_id",
        "move_type",
        F.date_trunc("month", "moved_at").alias("moved_at_month"),
    )
    .agg(
        F.round(F.avg("unit_price_adj_bucket_bucket"), 2).alias("avg_unit_price_adj_bucket_bucket"),
        F.min("moved_at").alias("first_moved_at"),
        F.count("*").alias("row_count"),
        F.sum("unit_price_adj_bucket_bucket").alias("sum_unit_price_adj_bucket_bucket_check"),
    )
    .filter(F.col("sum_unit_price_adj_bucket_bucket_check") > 226)
    .drop("sum_unit_price_adj_bucket_bucket_check")
)

result = (
    per_group
    .groupBy("row_count")
    .pivot("move_type", ["INBOUND", "OUTBOUND", "TRANSFER", "ADJUST"])
    .agg(F.sum("avg_unit_price_adj_bucket_bucket"))
)
