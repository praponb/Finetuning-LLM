from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "sku_id").orderBy(F.col("unit_price").desc(), F.col("id").desc())

cleaned_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "operator",
        "unit_price",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
    )
)

deduped = (
    cleaned_moves
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

screened = (
    deduped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.sku_id").isNotNull()) & (F.col("st.unit_price") > 215.4), "left_semi")
)

result = (
    screened
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "least_qty_unit_price",
        F.concat(F.col("operator"), F.lit("#"), F.col("qty").cast("string")).alias("operator_ref"),
        F.round(F.col("least_qty_unit_price") / F.nullif(F.col("unit_price"), F.lit(0)), 3).alias("least_qty_unit_price_per_unit_price"),
    )
)
