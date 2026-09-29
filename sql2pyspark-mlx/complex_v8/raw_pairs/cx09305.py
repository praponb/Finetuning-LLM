from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_unit_price = Window.orderBy("unit_price", "id")
w_moved_at_hour_to_date = Window.partitionBy(F.to_date("moved_at_hour"))
w_latest = Window.partitionBy("move_type", "warehouse_id").orderBy("moved_at_hour_to_date_avg_unit_price", "id")

valid_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
    )
)

ranked = (
    valid_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "moved_at",
        "moved_at_hour",
        F.round(F.percent_rank().over(w_all_by_unit_price), 3).alias("unit_price_percent_rank"),
        F.round(F.col("unit_price") - F.avg("unit_price").over(w_moved_at_hour_to_date), 2).alias("unit_price_vs_moved_at_hour_to_date_avg"),
        F.round(F.avg("unit_price").over(w_moved_at_hour_to_date), 2).alias("moved_at_hour_to_date_avg_unit_price"),
        F.round(F.cume_dist().over(w_all_by_unit_price), 3).alias("unit_price_cume_dist"),
        F.size(F.collect_set("warehouse_id").over(w_moved_at_hour_to_date)).alias("distinct_warehouse_id_in_moved_at_hour_to_date"),
        F.row_number().over(w_all_by_unit_price).alias("seq_no"),
    )
)

latest = (
    ranked
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_children = (
    latest.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.move_type",
        "s.moved_at",
        "s.unit_price_percent_rank",
        "s.moved_at_hour_to_date_avg_unit_price",
        "s.unit_price_cume_dist",
        "s.distinct_warehouse_id_in_moved_at_hour_to_date",
        "s.seq_no",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.move_type").alias("move_move_type"),
    )
)

enriched = (
    with_children
    .filter(F.col("move_type") == "OUTBOUND")
    .select(
        "*",
        F.abs(F.col("unit_price_percent_rank") - 0.4).alias("unit_price_percent_rank_distance"),
        F.round(F.col("unit_price_cume_dist") * 0.25, 2).alias("unit_price_cume_dist_adj"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
    )
)

labeled = (
    enriched
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.date_trunc("month", F.col("moved_at")).alias("moved_at_month"),
    )
)

result = (
    labeled
    .select(
        "id",
        "move_type",
        "moved_at_hour_to_date_avg_unit_price",
        "unit_price_cume_dist",
        "distinct_warehouse_id_in_moved_at_hour_to_date",
        "move_id",
        "move_qty",
        "unit_price_percent_rank_distance",
        "move_type_len",
        "move_type_slug",
    )
    .orderBy(F.col("unit_price_cume_dist").desc())
)
