from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("warehouse_id", "sku_id").orderBy(F.col("moved_at_quarter").desc(), F.col("id").desc())

base = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) | (F.col("sku_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
    )
)

unique_rows = (
    base
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

per_group = (
    unique_rows
    .groupBy("warehouse_id", "moved_at_quarter", "operator")
    .agg(
        F.percentile_approx("qty", 0.5).alias("median_qty"),
        F.round(F.stddev("qty"), 2).alias("stddev_qty"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
        F.max("unit_price").alias("max_unit_price_check"),
    )
    .filter(F.col("max_unit_price_check") > 60.4)
    .drop("max_unit_price_check")
)

detail_rows = (
    per_group.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.operator").isin("op_dew", "op_eve", "op_bee")), "left")
    .select(
        "s.warehouse_id",
        "s.moved_at_quarter",
        "s.operator",
        "s.stddev_qty",
        "s.operator_list",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.sku_id").alias("move_sku_id"),
    )
)

result = (
    detail_rows
    .select(
        "warehouse_id",
        "moved_at_quarter",
        "operator",
        "stddev_qty",
        "operator_list",
        "move_operator",
        "move_sku_id",
        F.concat(F.col("move_operator"), F.lit("#"), F.col("move_sku_id").cast("string")).alias("move_operator_ref"),
        F.coalesce(F.col("stddev_qty"), F.lit(0.0)).alias("stddev_qty_filled"),
    )
    .orderBy("operator", "warehouse_id")
)
