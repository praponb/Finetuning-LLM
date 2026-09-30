from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_operator = Window.partitionBy("move_type", "operator")
w_moved_at_plus_7d_month_sku_i = Window.partitionBy(F.date_trunc("month", "moved_at_plus_7d"), "sku_id").orderBy("qty", "id")

cleaned = (
    df_stock_moves
    .filter((F.col("unit_price") > 189.4) & (F.col("operator").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.count("*").alias("move_count"))
)

with_counts = (
    cleaned.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"))
)

windowed = (
    with_counts
    .select(
        "*",
        F.size(F.collect_set("sku_id").over(w_move_type_operator)).alias("distinct_sku_id_in_move_type_operator"),
        F.ntile(5).over(w_moved_at_plus_7d_month_sku_i).alias("qty_quintile"),
        (F.col("move_count") - F.lag("move_count").over(w_moved_at_plus_7d_month_sku_i)).alias("move_count_change"),
    )
)

result = (
    windowed
    .groupBy("sku_id")
    .pivot("move_type", ["TRANSFER", "ADJUST", "OUTBOUND", "INBOUND"])
    .agg(F.sum("qty_quintile"))
    .orderBy("sku_id")
)
