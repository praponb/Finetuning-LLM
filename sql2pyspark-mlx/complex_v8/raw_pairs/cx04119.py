from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_moved_at_month_by_qty_full = Window.partitionBy(F.month("moved_at")).orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_latest = Window.partitionBy("move_type").orderBy(F.col("qty").desc(), F.col("id").desc())

cleaned_moves = (
    df_stock_moves
    .filter((F.to_date(F.col("moved_at")) < F.lit("2025-02-24").cast("date")) & (F.col("unit_price").between(37.6, 313.8)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("moved_at")).alias("days_since_moved_at"),
        F.concat(F.col("operator"), F.lit("#"), F.col("sku_id").cast("string")).alias("operator_ref"),
        (F.col("moved_at") + F.expr("INTERVAL 7 DAYS")).alias("moved_at_plus_7d"),
    )
)

with_windows = (
    cleaned_moves
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "days_since_moved_at",
        "operator_ref",
        F.last("moved_at_plus_7d").over(w_moved_at_month_by_qty_full).alias("last_moved_at_plus_7d"),
        F.first("moved_at").over(w_moved_at_month_by_qty_full).alias("first_moved_at"),
    )
)

deduped = (
    with_windows
    .withColumn("row_num", F.row_number().over(w_latest))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupingSets(
        [["move_type", "warehouse_id"], ["move_type"], ["warehouse_id"]],
        "move_type", "warehouse_id",
    )
    .agg(F.count("*").alias("n_rows"))
    .select(
        F.coalesce(F.col("move_type"), F.lit("ALL")).alias("move_type"),
        "warehouse_id",
        "n_rows",
    )
    .orderBy(
        F.col("move_type").asc_nulls_last(),
        F.col("warehouse_id").asc_nulls_last(),
    )
)
