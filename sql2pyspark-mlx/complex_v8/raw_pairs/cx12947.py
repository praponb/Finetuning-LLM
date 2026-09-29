from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("operator", "warehouse_id").orderBy(F.col("qty").desc(), F.col("id").desc())

scoped = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.lower(F.trim(F.col("operator"))).alias("operator_clean"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("qty").cast("string")).alias("move_type_ref"),
    )
)

latest_per_key = (
    scoped
    .withColumn("recency_rank", F.row_number().over(w_latest))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

qualified = (
    latest_per_key.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.move_type").isin("ADJUST", "OUTBOUND")), "left_semi")
    .filter(F.col("unit_price") > 152.7)
)

result = (
    qualified
    .filter(F.col("move_type_ref").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator",
        "unit_price",
        "operator_clean",
        "move_type_ref",
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
        F.round(F.col("qty") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("qty_per_unit_price"),
    )
    .orderBy("operator", "id")
    .limit(10)
)
