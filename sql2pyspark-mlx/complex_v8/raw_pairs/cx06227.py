from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator", "move_type").orderBy(F.col("qty").desc(), F.col("id").desc())

base = (
    df_stock_moves
    .filter((F.col("operator") == "op_dew") & (F.col("unit_price").between(152.7, 398.5)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
    )
)

combined_details = (
    base.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        F.col("sk.labels").alias("sku_labels"),
    )
)

deduped = (
    combined_details
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

exploded = (
    deduped
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        "unit_price",
        F.explode("sku_labels").alias("sku_label"),
    )
)

joined = (
    exploded.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .filter(F.col("w.capacity") >= 2614)
    .select(
        "s.*",
        F.col("w.capacity").alias("warehouse_capacity"),
        F.col("w.wh_code").alias("warehouse_wh_code"),
        F.col("w.city").alias("warehouse_city"),
    )
)

move_stats = (
    df_stock_moves
    .groupBy("warehouse_id")
    .agg(
        F.min("qty").alias("min_move_qty"),
        F.max("moved_at").alias("last_moved_at"),
        F.max("moved_at").alias("last_moved_at_2"),
    )
)

summarized_rows = (
    joined.alias("s")
    .join(move_stats.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.*",
        "st.min_move_qty",
        "st.last_moved_at",
        "st.last_moved_at_2",
    )
)

exploded_detail = (
    summarized_rows.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.warehouse_id") == F.col("s.warehouse_id"), "left")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.sku_label",
        "s.min_move_qty",
        "s.last_moved_at_2",
        F.col("st.id").alias("move_id"),
        F.col("st.unit_price").alias("move_unit_price"),
        F.col("st.operator").alias("move_operator"),
    )
)

result = (
    exploded_detail
    .filter(F.col("min_move_qty") > 226)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "sku_label",
        "min_move_qty",
        "last_moved_at_2",
        "move_unit_price",
        "move_operator",
        F.round(F.col("move_unit_price") * 1.07, 2).alias("move_unit_price_adj"),
    )
    .orderBy(F.col("sku_label").asc_nulls_last())
)
