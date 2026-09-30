from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("sku_id").orderBy(F.col("moved_at").desc(), F.col("id").desc())
w_dedup_2 = Window.partitionBy("warehouse_city").orderBy(F.col("unit_price").desc(), F.col("id").desc())

scoped_moves = (
    df_stock_moves
    .filter(F.col("move_type") != "ADJUST")
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
        F.coalesce(F.col("unit_price"), F.lit(0.0)).alias("unit_price_filled"),
        F.round(F.col("unit_price") * 0.9, 2).alias("unit_price_adj"),
    )
)

unique_rows = (
    scoped_moves
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_parent = (
    unique_rows.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.moved_at",
        "s.operator",
        "s.unit_price",
        "s.move_type_slug",
        "s.unit_price_filled",
        "s.unit_price_adj",
        F.col("w.city").alias("warehouse_city"),
    )
)

latest_per_key = (
    with_parent
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

filtered = (
    latest_per_key.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.operator").isNotNull()) & (F.col("st.warehouse_id").isNotNull()), "left_anti")
)

deduped = (
    filtered
    .withColumn("recency_rank", F.row_number().over(w_dedup_2))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

result = (
    deduped
    .select(
        "id",
        "move_type",
        "moved_at",
        "operator",
        "move_type_slug",
        "unit_price_filled",
        "unit_price_adj",
        "warehouse_city",
        F.date_trunc("day", F.col("moved_at")).alias("moved_at_day"),
    )
    .orderBy(F.col("id").desc())
)
