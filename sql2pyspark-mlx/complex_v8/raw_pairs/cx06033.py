from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("move_type").orderBy(F.col("moved_at").desc(), F.col("id").desc())

prepared = df_stock_moves.filter(F.col("warehouse_id").isNotNull())

deduped = (
    prepared
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

line_level = (
    deduped.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.moved_at").alias("move_moved_at"),
    )
)

summary = (
    line_level
    .groupBy(
        F.date_format("move_moved_at", "yyyy-MM").alias("move_moved_at_ym"),
        "warehouse_id",
        "operator",
    )
    .agg(
        F.round(F.sum("move_unit_price") / F.nullif(F.sum("move_qty"), F.lit(0)), 3).alias("move_unit_price_to_move_qty_ratio"),
        F.percentile_approx("unit_price", 0.5).alias("median_unit_price"),
        F.count("*").alias("n_rows"),
        F.countDistinct("id").alias("distinct_ids"),
    )
    .filter(F.col("n_rows") >= 2)
)

result = (
    summary
    .select(
        "move_moved_at_ym",
        "warehouse_id",
        "operator",
        "median_unit_price",
        "n_rows",
        "distinct_ids",
    )
    .orderBy(F.col("median_unit_price").asc())
)
