from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_prior_rows_12h_sku_id_by_mov = Window.partitionBy("prior_rows_12h", "sku_id").orderBy(F.col("moved_at").desc(), "id")
w_prior_rows_12h_sku_id_by_mov_2 = Window.partitionBy("prior_rows_12h", "sku_id").orderBy(F.col("moved_at").desc())

covered = (
    df_stock_moves.alias("a")
    .join(
        df_stock_moves.alias("b"),
        (F.col("b.sku_id") == F.col("a.sku_id"))
        & (F.col("b.moved_at") < F.col("a.moved_at"))
        & (F.col("b.moved_at") >= F.col("a.moved_at") - F.expr("INTERVAL 12 HOURS")),
        "left",
    )
    .groupBy("a.id", "a.sku_id", "a.moved_at")
    .agg(
        F.count("b.id").alias("prior_rows_12h"),
        F.sum("b.qty").alias("prior_qty_12h"),
    )
)

metrics = (
    covered
    .withColumn("moved_at_quartile", F.ntile(4).over(w_prior_rows_12h_sku_id_by_mov))
    .withColumn("moved_at_cume_dist", F.round(F.cume_dist().over(w_prior_rows_12h_sku_id_by_mov_2), 3))
)

line_level = (
    metrics.alias("s")
    .join(df_stock_moves.alias("st"), F.col("st.sku_id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.sku_id",
        "s.prior_rows_12h",
        "s.prior_qty_12h",
        F.col("st.id").alias("move_id"),
        F.col("st.qty").alias("move_qty"),
        F.col("st.warehouse_id").alias("move_warehouse_id"),
    )
)

derived = (
    line_level
    .select(
        "*",
        (F.col("prior_qty_12h") - F.col("move_qty")).alias("prior_qty_12h_minus_move_qty"),
        (F.floor(F.col("prior_qty_12h") / 500) * 500).alias("prior_qty_12h_bucket"),
    )
)

result = (
    derived
    .select(
        "id",
        "sku_id",
        "prior_rows_12h",
        "prior_qty_12h",
        "move_id",
        "move_qty",
        "move_warehouse_id",
        "prior_qty_12h_minus_move_qty",
        "prior_qty_12h_bucket",
        F.round(F.col("prior_rows_12h") / F.nullif(F.col("prior_qty_12h_bucket"), F.lit(0)), 3).alias("prior_rows_12h_per_prior_qty_12h_bucket"),
    )
    .orderBy(F.col("prior_qty_12h").desc_nulls_first())
)
