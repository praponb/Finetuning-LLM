from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), F.col("id").desc())
w_dedup = Window.partitionBy("warehouse_id").orderBy(F.col("qty").desc(), F.col("id").desc())

valid_moves = (
    df_stock_moves
    .filter(F.to_date(F.col("moved_at")) < F.lit("2025-03-02").cast("date"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        (F.col("unit_price") - F.col("qty")).alias("unit_price_minus_qty"),
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

filtered = (
    valid_moves.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left_semi")
)

annotated = (
    filtered
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        F.concat(F.col("move_type_clean"), F.lit("#"), F.col("warehouse_id").cast("string")).alias("move_type_clean_ref"),
    )
)

unique_rows = (
    annotated
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

sku_id_stats = (
    unique_rows
    .groupBy("sku_id")
    .agg(F.max("qty").alias("max_qty"))
)

outliers = (
    unique_rows.alias("s")
    .join(sku_id_stats.alias("g"), F.col("g.sku_id") == F.col("s.sku_id"), "inner")
    .filter(F.col("s.qty") == F.col("g.max_qty"))
    .select(
        "s.*",
        "g.max_qty",
        F.round(F.col("s.qty") - F.col("g.max_qty"), 2).alias("qty_minus_max"),
    )
)

latest_per_key = (
    outliers
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

line_level_moves = (
    latest_per_key.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.moved_at",
        "s.unit_price",
        "s.move_type_clean_ref",
        "s.max_qty",
        "s.qty_minus_max",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        "st.move_type",
    )
)

result = (
    line_level_moves
    .filter(F.col("move_type").isNotNull())
    .select(
        "id",
        "sku_id",
        "unit_price",
        "qty_minus_max",
        "move_id",
        "move_warehouse_id",
        "move_type",
        F.coalesce(F.col("move_type"), F.lit("UNKNOWN")).alias("move_type_or_unknown"),
    )
    .orderBy(F.col("sku_id").desc_nulls_first())
)
