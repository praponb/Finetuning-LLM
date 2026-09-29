from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "sku_id").orderBy("unit_price_per_qty", "id")

prepared_moves = (
    df_stock_moves
    .filter(F.col("moved_at") < F.lit("2025-03-02 09:00:00").cast("timestamp"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
        F.concat_ws("|", F.col("move_type"), F.col("operator")).alias("move_type_operator_label"),
    )
)

derived = (
    prepared_moves
    .filter(F.col("unit_price_per_qty") > 1.5)
    .withColumn("unit_price_per_qty_distance", F.abs(F.col("unit_price_per_qty") - 4.5))
)

qualified = (
    derived.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.hour(F.col("st.moved_at")).between(6, 11)) & (F.col("st.sku_id").isNotNull()), "left_anti")
)

deduped = (
    qualified
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupBy("moved_at", "sku_id")
    .pivot("operator", ["op_ann", "op_bee", "op_chan"])
    .agg(F.min("unit_price_per_qty"))
)
