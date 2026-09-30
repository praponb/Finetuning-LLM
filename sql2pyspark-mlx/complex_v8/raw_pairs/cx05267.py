from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("avg_qty").desc(), F.col("id").desc())

base_moves = (
    df_stock_moves
    .filter((F.col("moved_at") >= F.lit("2025-02-12 10:00:00").cast("timestamp")) | (F.col("qty") >= 91))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

annotated = base_moves.withColumn("operator_len", F.length(F.trim(F.col("operator"))))

warehouse_id_stats = (
    annotated
    .groupBy("warehouse_id")
    .agg(F.avg("qty").alias("avg_qty"))
)

above_average = (
    annotated.alias("s")
    .join(warehouse_id_stats.alias("g"), F.col("g.warehouse_id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("s.qty") > F.col("g.avg_qty"))
    .select(
        "s.*",
        "g.avg_qty",
        F.round(F.col("s.qty") - F.col("g.avg_qty"), 2).alias("qty_minus_avg"),
    )
)

joined = (
    above_average.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.city").isNotNull())
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.operator_len",
        "s.avg_qty",
        "s.qty_minus_avg",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

derived = (
    joined
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "operator_len",
        "avg_qty",
        "warehouse_wh_code",
        "warehouse_city",
        F.coalesce(F.col("warehouse_city"), F.lit("UNKNOWN")).alias("warehouse_city_or_unknown"),
    )
)

unique_rows = (
    derived
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator_len",
        "avg_qty",
        "warehouse_wh_code",
        "warehouse_city",
        "warehouse_city_or_unknown",
        F.initcap(F.trim(F.col("warehouse_wh_code"))).alias("warehouse_wh_code_clean"),
    )
    .orderBy("id")
)
