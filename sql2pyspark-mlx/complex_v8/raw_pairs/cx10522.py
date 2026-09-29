from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("operator").orderBy(F.col("greatest_unit_price_qty").desc(), F.col("id").desc())

base_moves = (
    df_stock_moves
    .filter(F.col("sku_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
    )
)

enriched = (
    base_moves
    .select(
        "*",
        F.least(F.col("qty"), F.col("unit_price")).alias("least_qty_unit_price"),
        F.greatest(F.col("unit_price"), F.col("qty")).alias("greatest_unit_price_qty"),
    )
)

screened = (
    enriched.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.qty") <= 445), "left_anti")
    .filter(F.col("operator") != "op_ann")
)

latest = (
    screened
    .withColumn("recency_rank", F.row_number().over(w_dedup))
    .filter(F.col("recency_rank") == 1)
    .drop("recency_rank")
)

move_stats = (
    df_stock_moves
    .groupBy("sku_id")
    .agg(
        F.min("moved_at").alias("first_moved_at"),
        F.min("moved_at").alias("first_moved_at_2"),
    )
)

with_counts = (
    latest.alias("s")
    .join(move_stats.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "left")
    .select("s.*", "st.first_moved_at", "st.first_moved_at_2")
)

result = (
    with_counts
    .filter(F.col("least_qty_unit_price").between(175.7, 450.1))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "least_qty_unit_price",
        "first_moved_at",
        F.round((F.unix_timestamp(F.col("first_moved_at_2")) - F.unix_timestamp(F.col("moved_at"))) / 60, 1).alias("minutes_moved_at_to_first_moved_at_2"),
    )
    .orderBy("first_moved_at")
)
