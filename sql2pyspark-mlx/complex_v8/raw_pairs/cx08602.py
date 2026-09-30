from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "move_type").orderBy("unit_price_minus_qty", "id")
w_move_type_operator_by_unit_p = Window.partitionBy("move_type", "operator").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id = Window.partitionBy("warehouse_id")
w_moved_at_to_date_by_moved_at = Window.partitionBy(F.to_date("moved_at")).orderBy(F.col("moved_at").asc_nulls_last(), "id")

base_moves = (
    df_stock_moves
    .filter(F.col("operator") == "op_bee")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.concat(F.col("operator"), F.lit("#"), F.col("sku_id").cast("string")).alias("operator_ref"),
    )
)

deduped = (
    base_moves
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.min("moved_at").alias("first_moved_at"))
)

profiled = (
    deduped.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.first_moved_at")
)

sequenced = (
    profiled
    .select(
        "id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        "move_type_clean",
        "operator_ref",
        "first_moved_at",
        F.first("unit_price").over(w_move_type_operator_by_unit_p).alias("first_unit_price"),
        F.round(F.col("unit_price_minus_qty") / F.nullif(F.sum("unit_price_minus_qty").over(w_warehouse_id), F.lit(0)), 4).alias("unit_price_minus_qty_share_of_warehouse_id"),
        F.max("qty").over(w_warehouse_id).alias("warehouse_id_max_qty"),
        F.round(F.col("qty") - F.avg("qty").over(w_warehouse_id), 2).alias("qty_vs_warehouse_id_avg"),
        F.row_number().over(w_moved_at_to_date_by_moved_at).alias("seq_no"),
    )
)

with_activity = (
    sequenced
    .crossJoin(sequenced.agg(F.max("qty_vs_warehouse_id_avg").alias("overall_max_qty_vs_warehouse_id_avg")))
    .filter(F.col("qty_vs_warehouse_id_avg") < F.col("overall_max_qty_vs_warehouse_id_avg"))
    .drop("overall_max_qty_vs_warehouse_id_avg")
)

top_rows = with_activity.filter((F.col("seq_no") == 1) & (F.col("move_type") == "INBOUND"))

result = (
    top_rows
    .select(
        "id",
        "moved_at",
        "operator",
        "move_type_clean",
        "first_unit_price",
        "unit_price_minus_qty_share_of_warehouse_id",
        "warehouse_id_max_qty",
        "seq_no",
        F.dayofweek(F.col("moved_at")).alias("moved_at_dayofweek"),
        F.when(F.col("seq_no") >= 7, "HIGH").when(F.col("seq_no") >= 3, "MEDIUM").otherwise("LOW").alias("seq_no_tier"),
    )
    .orderBy(F.col("seq_no_tier").desc())
)
