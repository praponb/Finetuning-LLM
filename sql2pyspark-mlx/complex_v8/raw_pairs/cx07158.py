from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id_by_moved_at = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc(), "id")
w_operator_or_unknown_by_unit = Window.partitionBy("operator_or_unknown").orderBy(F.col("unit_price").desc(), "id")
w_warehouse_id_by_moved_at_ful = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_warehouse_id_by_moved_at_run = Window.partitionBy("warehouse_id").orderBy(F.col("moved_at").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_moved_at_to_date_warehouse_i = Window.partitionBy(F.to_date("moved_at"), "warehouse_id")

filtered_moves = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_trunc("hour", F.col("moved_at")).alias("moved_at_hour"),
        F.coalesce(F.col("operator"), F.lit("UNKNOWN")).alias("operator_or_unknown"),
    )
)

qualified = (
    filtered_moves.alias("s")
    .join(df_warehouses.alias("w"), (F.col("w.id") == F.col("s.warehouse_id")) & (F.col("w.opened_on").between(F.lit("2024-02-19").cast("date"), F.lit("2025-02-14").cast("date"))) & (F.lower(F.col("w.wh_code")).like("%kk%")), "left_semi")
)

windowed = (
    qualified
    .select(
        "*",
        F.lag("moved_at", 3).over(w_warehouse_id_by_moved_at).alias("prev3_moved_at"),
        F.lead("moved_at_hour").over(w_operator_or_unknown_by_unit).alias("next_moved_at_hour"),
        F.round(F.percent_rank().over(w_operator_or_unknown_by_unit), 3).alias("unit_price_percent_rank"),
        F.nth_value("qty", 2).over(w_warehouse_id_by_moved_at_ful).alias("second_qty"),
        F.last("moved_at_hour", ignorenulls=True).over(w_warehouse_id_by_moved_at_run).alias("last_known_moved_at_hour"),
    )
)

metrics = (
    windowed
    .withColumn("distinct_move_type_in_moved_at_to_date_warehouse", F.size(F.collect_set("move_type").over(w_moved_at_to_date_warehouse_i)))
    .withColumn("qty_share_of_moved_at_to_date_warehouse_id", F.round(F.col("qty") / F.nullif(F.sum("qty").over(w_moved_at_to_date_warehouse_i), F.lit(0)), 4))
)

result = (
    metrics
    .select(
        "id",
        F.col("unit_price_percent_rank").alias("m_unit_price_percent_rank"),
        F.col("qty_share_of_moved_at_to_date_warehouse_id").alias("qty_share_of_moved_at_to_date_warehouse_id_v"),
    )
    .unpivot(
        ids=["id"],
        values=["m_unit_price_percent_rank", "qty_share_of_moved_at_to_date_warehouse_id_v"],
        variableColumnName="field",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("id", "field")
)
