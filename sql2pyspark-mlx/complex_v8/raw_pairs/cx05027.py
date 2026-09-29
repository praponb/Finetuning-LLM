from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id", "unit_price_tier").orderBy("moved_at", "id")
w_dedup_2 = Window.partitionBy("warehouse_id").orderBy("move_count", "id")

recent_moves = (
    df_stock_moves
    .filter(F.col("operator").isin("op_eve", "op_bee"))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "qty",
        "moved_at",
        "unit_price",
        (F.col("moved_at") + F.expr("INTERVAL 1 DAYS")).alias("moved_at_plus_1d"),
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
        F.when(F.col("unit_price") >= 323.9, "A").when(F.col("unit_price") >= 189.4, "B").otherwise("C").alias("unit_price_tier"),
    )
)

filtered = (
    recent_moves.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.unit_price") <= 330.9) & (F.col("st.qty") < 355), "left_anti")
    .filter(F.col("warehouse_id").isNotNull())
)

deduped = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.max("moved_at").alias("last_moved_at"),
        F.count("*").alias("move_count"),
    )
)

profiled = (
    deduped.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        "st.first_moved_at",
        "st.last_moved_at",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

deduped_2 = (
    profiled
    .withColumn("row_num", F.row_number().over(w_dedup_2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

enriched_warehouse = (
    deduped_2.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .filter(F.col("w.wh_code").isNotNull())
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.qty",
        "s.moved_at_plus_1d",
        "s.first_moved_at",
        "s.last_moved_at",
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.opened_on").alias("warehouse_opened_on"),
        "sk.sku_code",
        F.col("sk.weight_kg").alias("sku_weight_kg"),
    )
)

result = (
    enriched_warehouse
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "moved_at_plus_1d",
        "first_moved_at",
        "last_moved_at",
        "warehouse_wh_code",
        "warehouse_opened_on",
        "sku_code",
        (F.floor(F.col("qty") / 100) * 100).alias("qty_bucket"),
        (F.col("last_moved_at") + F.expr("INTERVAL 1 DAYS")).alias("last_moved_at_plus_1d"),
    )
)
