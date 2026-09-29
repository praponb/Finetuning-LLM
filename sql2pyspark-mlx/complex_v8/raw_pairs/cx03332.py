from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_sku_id_by_qty = Window.partitionBy("move_type", "sku_id").orderBy("qty", "id")
w_move_type_sku_id_by_qty_runn = Window.partitionBy("move_type", "sku_id").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

prepared = df_stock_moves.filter((F.col("warehouse_id") >= 5) & (F.col("sku_id").isNotNull()))

combined_details = (
    prepared.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select("s.*", "sk.sku_code", F.col("sk.category").alias("sku_category"))
)

with_trends = (
    combined_details
    .withColumn("qty_cume_dist", F.round(F.cume_dist().over(w_move_type_sku_id_by_qty), 3))
    .withColumn("running_count", F.count(F.lit(1)).over(w_move_type_sku_id_by_qty_runn))
)

with_details = (
    with_trends.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.moved_at",
        "s.unit_price",
        "s.sku_code",
        "s.qty_cume_dist",
        "s.running_count",
        F.col("w.city").alias("warehouse_city"),
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
    )
)

tagged = (
    with_details
    .select(
        "*",
        F.coalesce(F.col("warehouse_city"), F.lit("UNKNOWN")).alias("warehouse_city_or_unknown"),
        F.lower(F.trim(F.col("sku_code"))).alias("sku_code_clean"),
    )
)

with_activity = (
    tagged.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.qty") < 288) & (F.col("st.unit_price") >= 209.4), "left_anti")
)

result = (
    with_activity
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        "qty_cume_dist",
        "running_count",
        "warehouse_capacity",
        "warehouse_wh_code",
        "warehouse_city_or_unknown",
    )
    .orderBy(F.col("warehouse_id").desc(), F.col("id").desc())
    .limit(10)
)
