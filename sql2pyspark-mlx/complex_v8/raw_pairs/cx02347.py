from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")

valid_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("unit_price") >= 298.9, "large").when(F.col("unit_price") >= 238.5, "medium").otherwise("small").alias("unit_price_tier"),
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        (F.col("qty") + F.col("unit_price")).alias("qty_plus_unit_price"),
    )
)

screened = (
    valid_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") > 238.5) & (F.col("st.operator").isin("op_bee", "op_chan", "op_ann")), "left_anti")
    .filter(F.col("qty") < 422)
)

filtered = (
    screened
    .crossJoin(screened.agg(F.avg("qty_plus_unit_price").alias("overall_avg_qty_plus_unit_price")))
    .filter(F.col("qty_plus_unit_price") > F.col("overall_avg_qty_plus_unit_price"))
    .drop("overall_avg_qty_plus_unit_price")
)

scored = (
    filtered
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "*",
        F.round(F.col("qty_plus_unit_price") * 1.5, 2).alias("qty_plus_unit_price_adj"),
        F.concat(F.col("unit_price_tier"), F.lit("#"), F.col("qty").cast("string")).alias("unit_price_tier_ref"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty_plus_unit_price"), F.lit(0)), 3).alias("unit_price_per_qty_plus_unit_price"),
    )
)

with_windows = (
    scored
    .withColumn("sku_id_max_unit_price", F.max("unit_price").over(w_sku_id))
    .withColumn("rows_in_sku_id", F.count(F.lit(1)).over(w_sku_id))
)

result = (
    with_windows
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "unit_price_tier",
        "qty_plus_unit_price",
        "qty_plus_unit_price_adj",
        "unit_price_per_qty_plus_unit_price",
        "sku_id_max_unit_price",
        "rows_in_sku_id",
        F.lower(F.trim(F.col("unit_price_tier_ref"))).alias("unit_price_tier_ref_clean"),
        F.date_add(F.col("moved_at_date"), 90).alias("moved_at_date_plus_90d"),
    )
    .orderBy(F.col("unit_price_tier_ref_clean").desc())
)
