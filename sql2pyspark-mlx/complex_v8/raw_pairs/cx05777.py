from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("move_type").orderBy("qty_distance", "id")

cleaned = (
    df_stock_moves
    .filter(F.col("qty") < 332)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

derived = (
    cleaned
    .select(
        "*",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        F.lower(F.regexp_replace(F.trim(F.col("move_type_slug")), " ", "_")).alias("move_type_slug_slug"),
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
    )
)

derived_2 = (
    derived
    .select(
        "id",
        "sku_id",
        "move_type",
        "qty",
        "move_type_slug",
        "move_type_slug_slug",
        "moved_at_year",
        F.abs(F.col("qty") - 314).alias("qty_distance"),
        F.length(F.trim(F.col("move_type_slug"))).alias("move_type_slug_len"),
        F.when(F.col("qty") >= 355, "A").when(F.col("qty") >= 168, "B").otherwise("C").alias("qty_tier"),
    )
)

unique_rows = (
    derived_2
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_children = (
    unique_rows.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.move_type_slug_slug",
        "s.moved_at_year",
        "s.qty_distance",
        "s.move_type_slug_len",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.unit_price").alias("move_unit_price"),
    )
)

detail_rows_moves = (
    with_children.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.move_warehouse_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.move_type",
        "s.move_type_slug_slug",
        "s.qty_distance",
        "s.move_id",
        "s.move_warehouse_id",
        "s.move_moved_at",
        "s.move_unit_price",
        F.col("st.id").alias("move_id_2"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.unit_price").alias("move_unit_price_2"),
    )
)

result = (
    detail_rows_moves
    .select(
        "id",
        "sku_id",
        "move_type",
        "move_type_slug_slug",
        "qty_distance",
        "move_id",
        "move_moved_at",
        "move_id_2",
        "move_operator",
        "move_unit_price_2",
        F.abs(F.col("move_unit_price_2") - 258.6).alias("move_unit_price_2_distance"),
        F.datediff(F.lit("2025-06-30").cast("date"), F.col("move_moved_at")).alias("days_since_move_moved_at"),
    )
    .orderBy(F.col("sku_id").desc())
)
