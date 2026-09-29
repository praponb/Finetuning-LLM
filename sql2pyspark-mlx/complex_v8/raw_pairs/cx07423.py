from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_moved_at = Window.orderBy("moved_at", "id")
w_move_type = Window.partitionBy("move_type")
w_latest = Window.partitionBy("move_type").orderBy(F.col("unit_price").desc(), F.col("id").desc())
w_all_by_move_move_type_count = Window.orderBy(F.col("move_move_type_count").desc(), "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_move_id_by_move_mo = Window.partitionBy("move_type", "move_id").orderBy("move_move_type_count", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_move_type_count = Window.partitionBy("move_move_type_count")
w_move_id_by_move_move_type_co = Window.partitionBy("move_id").orderBy("move_move_type_count", "id", "move_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_all_by_move_move_type_count_2 = Window.orderBy(F.col("move_move_type_count").desc(), "id", "move_id")

recent = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_bee", "op_chan"))
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

metrics = (
    recent
    .select(
        "*",
        F.lag("moved_at", 3).over(w_all_by_moved_at).alias("prev3_moved_at"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_move_type), F.lit(0)), 4).alias("unit_price_share_of_move_type"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_move_type), 2).alias("unit_price_vs_move_type_avg"),
    )
)

latest_per_key = (
    metrics
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

exploded_detail_moves = (
    latest_per_key.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id").isNotNull()), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.sku_id").alias("move_sku_id"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.count("*").alias("move_count"),
        F.count("*").alias("move_count_2"),
        F.count(F.when(F.col("move_type") == "TRANSFER", 1)).alias("move_move_type_count"),
    )
)

with_counts = (
    exploded_detail_moves.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.move_sku_id"), "left")
    .select(
        "s.*",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_count_2"), F.lit(0)).alias("move_count_2"),
        F.coalesce(F.col("st.move_move_type_count"), F.lit(0)).alias("move_move_type_count"),
    )
)

ranked = (
    with_counts
    .select(
        "id",
        "warehouse_id",
        "qty",
        "moved_at",
        "operator",
        "move_id",
        "move_sku_id",
        "move_move_type",
        "move_count",
        "move_move_type_count",
        F.min("unit_price").over(w_all_by_move_move_type_count).alias("min_unit_price_so_far"),
        F.last("move_move_type_count").over(w_move_type_move_id_by_move_mo).alias("last_move_move_type_count"),
        F.max("move_count_2").over(w_move_move_type_count).alias("move_move_type_count_max_move_count_2"),
        F.count(F.lit(1)).over(w_move_move_type_count).alias("rows_in_move_move_type_count"),
        F.first("moved_at", ignorenulls=True).over(w_move_id_by_move_move_type_co).alias("first_known_moved_at"),
        F.rank().over(w_all_by_move_move_type_count_2).alias("move_move_type_count_rank"),
    )
)

per_group = (
    ranked
    .groupBy(
        "last_move_move_type_count",
        "move_move_type",
        F.date_trunc("month", "first_known_moved_at").alias("first_known_moved_at_month"),
    )
    .agg(
        F.round(F.sum("min_unit_price_so_far"), 2).alias("total_min_unit_price_so_far"),
        F.min("moved_at").alias("first_moved_at"),
        F.max("move_count").alias("max_move_count_check"),
    )
    .filter(F.col("max_move_count_check") > 2)
    .drop("max_move_count_check")
)

result = (
    per_group
    .select(
        "last_move_move_type_count",
        "move_move_type",
        "first_known_moved_at_month",
        "total_min_unit_price_so_far",
        "first_moved_at",
        F.coalesce(F.col("move_move_type"), F.lit("UNKNOWN")).alias("move_move_type_or_unknown"),
    )
    .orderBy(F.col("first_moved_at").desc())
)
