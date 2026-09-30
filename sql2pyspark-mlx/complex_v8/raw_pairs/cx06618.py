from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "sku_id").orderBy(F.col("unit_price").desc(), F.col("id").desc())

filtered = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at",
        "operator",
        "unit_price",
    )
)

unique_rows = (
    filtered
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched = (
    unique_rows.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", "sk.sku_code")
)

without_matches = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left_semi")
)

with_details_warehouse = (
    without_matches.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select("s.*", F.col("w.wh_code").alias("warehouse_wh_code"))
)

result = (
    with_details_warehouse
    .select(
        "id",
        "moved_at",
        "unit_price",
        "sku_code",
        F.date_trunc("week", F.col("moved_at")).alias("moved_at_week"),
        F.date_trunc("year", F.col("moved_at")).alias("moved_at_year"),
    )
    .orderBy(F.col("id").desc())
    .limit(20)
)
