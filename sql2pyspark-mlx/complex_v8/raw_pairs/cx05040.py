from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator")
w_sku_id_by_unit_price_running = Window.partitionBy("sku_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared_moves = (
    df_stock_moves
    .filter((F.col("sku_id").isNotNull()) & (F.col("unit_price") < 273.5))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
    )
)

move_type_stats = (
    prepared_moves
    .groupBy("move_type")
    .agg(F.percentile_approx("unit_price", 0.5).alias("median_unit_price"))
)

above_average = (
    prepared_moves.alias("s")
    .join(move_type_stats.alias("g"), F.col("g.move_type") == F.col("s.move_type"), "inner")
    .filter(F.col("s.unit_price") >= F.col("g.median_unit_price"))
    .select(
        "s.*",
        "g.median_unit_price",
        F.round(F.col("s.unit_price") - F.col("g.median_unit_price"), 2).alias("unit_price_minus_median"),
    )
)

labeled = (
    above_average
    .withColumn("median_unit_price_filled", F.coalesce(F.col("median_unit_price"), F.lit(0.0)))
)

metrics = (
    labeled
    .withColumn("unit_price_vs_operator_avg", F.round(F.col("unit_price") - F.avg("unit_price").over(w_operator), 2))
    .withColumn("running_count", F.count(F.lit(1)).over(w_sku_id_by_unit_price_running))
)

with_children = (
    metrics.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") >= 124), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.median_unit_price",
        "s.unit_price_vs_operator_avg",
        F.col("st.id").alias("move_id"),
        F.col("st.move_type").alias("move_move_type"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.operator").alias("move_operator"),
    )
)

with_children_2 = (
    with_children.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.median_unit_price",
        "s.move_id",
        F.col("st.id").alias("move_id_2"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

result = (
    with_children_2
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "median_unit_price",
        "move_id",
        "move_id_2",
        "move_moved_at",
        "move_unit_price",
    )
    .orderBy(
        F.col("move_type").asc(),
        "id",
        "move_id",
        "move_id_2",
    )
    .limit(10)
)
