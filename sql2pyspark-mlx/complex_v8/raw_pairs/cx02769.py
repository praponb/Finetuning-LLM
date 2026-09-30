from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_sku_id = Window.partitionBy("sku_id")
w_sku_id_warehouse_id_by_unit = Window.partitionBy("sku_id", "warehouse_id").orderBy("unit_price_per_qty")
w_sku_id_move_type_by_unit_pri = Window.partitionBy("sku_id", "move_type").orderBy("unit_price")
w_sku_id_move_type_by_unit_pri_2 = Window.partitionBy("sku_id", "move_type").orderBy("unit_price", "id")

scoped = (
    df_stock_moves
    .filter(F.col("unit_price") > 95.8)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "unit_price",
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref"),
        F.concat(F.col("move_type"), F.lit("#"), F.col("sku_id").cast("string")).alias("move_type_ref_2"),
        F.round(F.col("unit_price") / F.nullif(F.col("qty"), F.lit(0)), 3).alias("unit_price_per_qty"),
    )
)

without_matches = (
    scoped.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.warehouse_id") == F.col("s.warehouse_id")) & (F.col("st.move_type").isNotNull()) & (F.col("st.sku_id").isNotNull()), "left_anti")
    .filter(F.col("qty") < 355)
)

metrics = (
    without_matches
    .select(
        "*",
        F.size(F.collect_set("move_type").over(w_sku_id)).alias("distinct_move_type_in_sku_id"),
        F.round(F.percent_rank().over(w_sku_id_warehouse_id_by_unit), 3).alias("unit_price_per_qty_percent_rank"),
        F.dense_rank().over(w_sku_id_move_type_by_unit_pri).alias("unit_price_dense_rank"),
        F.max("unit_price").over(w_sku_id).alias("sku_id_max_unit_price"),
        F.row_number().over(w_sku_id_move_type_by_unit_pri_2).alias("unit_price_seq"),
        F.min("unit_price").over(w_sku_id).alias("sku_id_min_unit_price"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(F.round(F.avg("qty"), 2).alias("avg_move_qty"))
)

summarized_rows = (
    metrics.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.avg_move_qty")
)

labeled = (
    summarized_rows
    .withColumn("avg_move_qty_tier", F.when(F.col("avg_move_qty") >= 400.2, "HIGH").when(F.col("avg_move_qty") >= 200.6, "MEDIUM").otherwise("LOW"))
)

enriched_warehouse = (
    labeled.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.unit_price",
        "s.move_type_ref",
        "s.unit_price_per_qty",
        "s.distinct_move_type_in_sku_id",
        "s.unit_price_per_qty_percent_rank",
        "s.unit_price_dense_rank",
        "s.sku_id_max_unit_price",
        "s.unit_price_seq",
        "s.sku_id_min_unit_price",
        "s.avg_move_qty",
        "s.avg_move_qty_tier",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
        "sk.sku_code",
    )
)

result = (
    enriched_warehouse
    .select(
        "id",
        F.col("unit_price_seq").alias("unit_price_seq_v"),
        F.col("qty").alias("m_qty"),
        F.col("distinct_move_type_in_sku_id").alias("distinct_move_type_in_sku_id_v"),
        F.col("unit_price_dense_rank").alias("m_unit_price_dense_rank"),
    )
    .unpivot(
        ids=["id"],
        values=["unit_price_seq_v", "m_qty", "distinct_move_type_in_sku_id_v", "m_unit_price_dense_rank"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
    .orderBy("id", "field")
)
