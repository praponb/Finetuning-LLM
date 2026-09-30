from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "warehouse_id").orderBy("moved_at", "id")
w_warehouse_city_operator_by_f = Window.partitionBy("warehouse_city", "operator").orderBy(F.col("first_moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_warehouse_city_operator_by_f_2 = Window.partitionBy("warehouse_city", "operator").orderBy(F.col("first_moved_at").desc(), "id")
w_first_moved_at_to_date = Window.partitionBy(F.to_date("first_moved_at"))
w_warehouse_city_operator_by_f_3 = Window.partitionBy("warehouse_city", "operator").orderBy(F.col("first_moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest_2 = Window.partitionBy("warehouse_city", "sku_id").orderBy(F.col("seq_no").desc(), F.col("id").desc())

base = df_stock_moves.filter(F.col("move_type").isin("ADJUST", "INBOUND", "TRANSFER"))

combined_details = (
    base.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.upper(F.trim(F.col("w.wh_code"))).like("K%"))
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

with_activity = (
    combined_details.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.moved_at") >= F.lit("2025-02-06 08:00:00").cast("timestamp")), "left_anti")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.max("moved_at").alias("last_moved_at"),
    )
)

with_stats = (
    with_activity.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select("s.*", "st.first_moved_at", "st.last_moved_at")
)

latest_per_key = (
    with_stats
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

ranked = (
    latest_per_key
    .select(
        "*",
        F.sum("qty").over(w_warehouse_city_operator_by_f).alias("running_qty"),
        F.row_number().over(w_warehouse_city_operator_by_f_2).alias("seq_no"),
        F.round(F.col("unit_price") / F.nullif(F.sum("unit_price").over(w_first_moved_at_to_date), F.lit(0)), 4).alias("unit_price_share_of_first_moved_at_to_date"),
        F.last("first_moved_at").over(w_warehouse_city_operator_by_f_3).alias("last_first_moved_at"),
        F.round(F.avg("qty").over(w_first_moved_at_to_date), 2).alias("first_moved_at_to_date_avg_qty"),
    )
)

latest_per_key_2 = (
    ranked
    .withColumn("row_num", F.row_number().over(w_latest_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    latest_per_key_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "warehouse_city",
        "first_moved_at",
        "last_moved_at",
        "seq_no",
        "unit_price_share_of_first_moved_at_to_date",
        "last_first_moved_at",
        "first_moved_at_to_date_avg_qty",
    )
    .orderBy("first_moved_at_to_date_avg_qty")
)
