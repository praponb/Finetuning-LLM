from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("warehouse_city").orderBy(F.col("move_operator_count").desc(), F.col("id").desc())

filtered = (
    df_stock_moves
    .filter(~F.col("operator").isin("op_chan", "op_dew"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        (F.floor(F.col("unit_price") / 100) * 100).alias("unit_price_bucket"),
        F.concat_ws("-", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
        F.count(F.when(F.col("operator").isNotNull(), 1)).alias("move_operator_count"),
    )
)

summarized_rows = (
    filtered.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
        F.coalesce(F.col("st.move_operator_count"), F.lit(0)).alias("move_operator_count"),
    )
)

with_activity = (
    summarized_rows.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.unit_price") <= 323.9) & (F.month(F.col("st.moved_at")).isin(1, 2)), "left_semi")
)

joined = (
    with_activity.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        "sk.sku_code",
    )
)

unique_rows = (
    joined
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

per_group = (
    unique_rows
    .filter(F.col("warehouse_city").isin("Bangkok", "Singapore"))
    .groupBy("sku_id", "move_operator_count")
    .agg(
        F.sum("move_count").alias("total_move_count"),
        F.max("move_operator_count").alias("max_move_operator_count"),
        F.percentile_approx("unit_price", 0.5).alias("median_unit_price"),
        F.round(F.avg("qty"), 2).alias("avg_qty"),
    )
)

result = (
    per_group
    .select(
        "sku_id",
        "move_operator_count",
        "total_move_count",
        "max_move_operator_count",
        "median_unit_price",
        "avg_qty",
        F.abs(F.col("median_unit_price") - 225.6).alias("median_unit_price_distance"),
    )
    .orderBy(F.col("total_move_count").asc(), "sku_id", "move_operator_count")
    .limit(5)
)
