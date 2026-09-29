from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_all_by_qty_ties = Window.orderBy(F.col("qty").desc())
w_move_type_operator_by_qty_fi = Window.partitionBy("move_type", "operator").orderBy(F.col("qty_filled").asc_nulls_last(), "id")
w_all_by_qty_running = Window.orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

recent = (
    df_stock_moves
    .filter((F.col("qty").between(90, 349)) & (F.col("move_type") == "INBOUND"))
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

scored = (
    recent
    .filter(F.col("operator").isin("op_chan", "op_dew", "op_ann"))
    .select(
        "*",
        F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"),
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
        F.coalesce(F.col("qty"), F.lit(0)).alias("qty_filled"),
    )
)

ranked = (
    scored
    .select(
        "id",
        "move_type",
        "qty",
        "operator",
        "move_type_clean",
        "least_unit_price_qty",
        "qty_filled",
        F.dense_rank().over(w_all_by_qty_ties).alias("qty_dense_rank"),
        (F.col("least_unit_price_qty") - F.lag("least_unit_price_qty").over(w_move_type_operator_by_qty_fi)).alias("least_unit_price_qty_change"),
        F.last("least_unit_price_qty", ignorenulls=True).over(w_all_by_qty_running).alias("last_known_least_unit_price_qty"),
    )
)

labeled = (
    ranked
    .filter(F.col("least_unit_price_qty_change") >= -350.0)
    .withColumn("qty_per_qty_dense_rank", F.round(F.col("qty") / F.nullif(F.col("qty_dense_rank"), F.lit(0)), 3))
)

result = (
    labeled
    .select(
        "id",
        "move_type",
        "qty",
        "operator",
        "move_type_clean",
        "qty_filled",
        "qty_dense_rank",
        "last_known_least_unit_price_qty",
        "qty_per_qty_dense_rank",
        F.when(F.col("operator").isin("op_dew", "op_bee"), "group_a").when(F.col("operator") == "op_ann", "group_b").otherwise("other").alias("operator_group"),
        F.round(F.col("qty_per_qty_dense_rank") * 0.25, 2).alias("qty_per_qty_dense_rank_adj"),
    )
    .orderBy(F.col("qty_dense_rank").desc())
)
