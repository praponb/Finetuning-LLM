from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id", "operator").orderBy(F.col("qty_bucket").desc(), F.col("id").desc())

filtered = (
    df_stock_moves
    .filter((~F.col("operator").isin("op_ann", "op_dew")) | (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.concat(F.col("moved_at_label"), F.lit("#"), F.col("sku_id").cast("string")).alias("moved_at_label_ref"),
        F.round(F.col("least_unit_price_qty") * 0.9, 2).alias("least_unit_price_qty_adj"),
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

latest = (
    scored
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(F.max("qty").alias("max_move_qty"), F.count("*").alias("move_count"))
)

with_counts = (
    latest.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.max_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

enriched = (
    with_counts
    .filter(F.col("moved_at") < F.lit("2025-03-08 09:00:00").cast("timestamp"))
    .select(
        "*",
        F.when(F.col("move_count") >= 6, "HIGH").when(F.col("move_count") >= 2, "MEDIUM").otherwise("LOW").alias("move_count_tier"),
        F.round(F.col("move_count") / F.nullif(F.col("qty_bucket"), F.lit(0)), 3).alias("move_count_per_qty_bucket"),
    )
)

result = (
    enriched
    .groupBy("moved_at")
    .pivot("operator", ["op_eve", "op_dew"])
    .agg(F.avg("least_unit_price_qty_adj"))
    .orderBy("moved_at")
)
