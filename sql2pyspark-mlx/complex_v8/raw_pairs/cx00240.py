from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "sku_id").orderBy(F.col("qty").desc(), F.col("id").desc())
w_latest = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), F.col("id").desc())

prepared_moves = (
    df_stock_moves
    .filter((F.col("unit_price") > 200.7) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.when(F.col("qty") >= 300, "HIGH").when(F.col("qty") >= 199, "MEDIUM").otherwise("LOW").alias("qty_tier"),
        F.substring(F.trim(F.col("operator")), 1, 3).alias("operator_prefix"),
    )
)

deduped = (
    prepared_moves
    .withColumn("row_num", F.row_number().over(w_dedup))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

unique_rows = (
    deduped
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.sum("qty").alias("total_move_qty"),
        F.max("qty").alias("max_move_qty"),
        F.count("*").alias("move_count"),
    )
)

profiled = (
    unique_rows.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.total_move_qty",
        "st.max_move_qty",
        F.coalesce(F.col("st.move_count"), F.lit(0)).alias("move_count"),
    )
)

joined_sku = (
    profiled.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.unit_price",
        "s.qty_tier",
        "s.operator_prefix",
        "s.max_move_qty",
        F.col("sk.category").alias("sku_category"),
        F.col("sk.is_active").alias("sku_is_active"),
        "sk.sku_code",
    )
)

exploded_detail_moves = (
    joined_sku.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.operator",
        "s.sku_is_active",
        "s.sku_code",
        F.col("st.id").alias("move_id"),
        F.col("st.operator").alias("move_operator"),
        F.col("st.moved_at").alias("move_moved_at"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

result = (
    exploded_detail_moves
    .groupingSets(
        [["sku_is_active", "warehouse_id"], ["sku_is_active"], ["warehouse_id"], []],
        "sku_is_active", "warehouse_id",
    )
    .agg(
        F.grouping("sku_is_active").alias("is_total_sku_is_active"),
        F.count("*").alias("cnt"),
        F.count(F.when(F.lower(F.col("sku_code")).like("%-7%"), 1)).alias("n_lower_sku_code_like_7"),
    )
)
