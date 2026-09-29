from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator").orderBy(F.col("moved_at").desc(), F.col("id").desc())

cleaned_moves = (
    df_stock_moves
    .filter((F.col("qty") < 314) & (F.col("operator") == "op_ann"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.greatest(F.col("qty"), F.col("unit_price")).alias("greatest_qty_unit_price"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

unique_rows = (
    cleaned_moves
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

tagged = (
    unique_rows
    .filter(F.col("unit_price") >= 180.0)
    .select(
        "*",
        (F.floor(F.col("greatest_qty_unit_price") / 100) * 100).alias("greatest_qty_unit_price_bucket"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown_2"),
        F.abs(F.col("greatest_qty_unit_price") - 250.5).alias("greatest_qty_unit_price_distance"),
    )
)

line_level = (
    tagged.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.greatest_qty_unit_price",
        "s.greatest_qty_unit_price_distance",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

result = (
    line_level
    .cube("move_type", "move_id", "operator")
    .agg(
        F.sum("move_qty").alias("total_move_qty"),
        F.array_sort(F.collect_set("operator")).alias("operator_list"),
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("move_id").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
    )
)
