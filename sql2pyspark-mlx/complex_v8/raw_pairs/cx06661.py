from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("move_type", "sku_id").orderBy(F.col("unit_price").desc(), F.col("id").desc())

scoped_moves = (
    df_stock_moves
    .filter((~F.col("move_type").isin("TRANSFER", "OUTBOUND")) & (F.month(F.col("moved_at")).isin(1, 2)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

latest = (
    scoped_moves
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

exploded_detail_moves = (
    latest.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.days_since_moved_at",
        "s.moved_at_label",
        F.col("st.id").alias("move_id"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
    )
)

combined_details = (
    exploded_detail_moves.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.move_id",
        "s.move_warehouse_id",
        "s.move_qty",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

without_matches = (
    combined_details.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type").isNotNull()), "left_semi")
)

tagged = (
    without_matches
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("sku_category")), " ", "_")).alias("sku_category_slug"),
    )
)

result = (
    tagged
    .select(
        "id",
        "warehouse_id",
        "qty",
        "move_id",
        "move_warehouse_id",
        "sku_weight_kg",
        "sku_category_slug",
        (F.floor(F.col("move_qty") / 100) * 100).alias("move_qty_bucket"),
    )
    .orderBy(F.col("warehouse_id").asc_nulls_last())
)
