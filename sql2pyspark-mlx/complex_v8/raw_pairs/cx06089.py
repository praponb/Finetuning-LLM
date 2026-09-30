from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("move_type", "warehouse_id").orderBy("unit_price", "id")
w_dedup_2 = Window.partitionBy("warehouse_id", "operator").orderBy(F.col("unit_price").desc(), F.col("id").desc())
w_operator_sku_id_by_moved_at = Window.partitionBy("operator", "sku_id").orderBy(F.col("moved_at").desc())
w_all_by_qty = Window.orderBy(F.col("qty").desc(), "id")
w_warehouse_id = Window.partitionBy("warehouse_id")
w_operator_sku_id_by_moved_at_2 = Window.partitionBy("operator", "sku_id").orderBy(F.col("moved_at").desc(), "id")
w_dedup_3 = Window.partitionBy("operator").orderBy(F.col("warehouse_id_sum_qty").desc(), F.col("id").desc())

scoped = (
    df_stock_moves
    .filter(F.col("operator") == "op_dew")
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
    scoped
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("operator")), " ", "_")).alias("operator_slug"),
    )
)

deduped = (
    enriched
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w_dedup_2))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    latest
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "operator_slug",
        F.rank().over(w_operator_sku_id_by_moved_at).alias("moved_at_rank"),
        F.round(F.cume_dist().over(w_all_by_qty), 3).alias("qty_cume_dist"),
        F.sum("qty").over(w_warehouse_id).alias("warehouse_id_sum_qty"),
        F.lead("unit_price").over(w_operator_sku_id_by_moved_at_2).alias("next_unit_price"),
    )
)

unique_rows = (
    with_windows
    .withColumn("row_num", F.row_number().over(w_dedup_3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    unique_rows
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "operator_slug",
        "moved_at_rank",
        "qty_cume_dist",
        "next_unit_price",
    )
    .orderBy("id", F.col("moved_at_rank").desc())
)
