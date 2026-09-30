from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator = Window.partitionBy("operator").orderBy("moved_at", "id")
w_operator_running = Window.partitionBy("operator").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_prev_moved_at_year = Window.partitionBy(F.year("prev_moved_at"))
w_move_type_starts_session_by = Window.partitionBy("move_type", "starts_session").orderBy(F.col("qty").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stock_moves
    .filter(F.col("sku_id") >= 3)
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "yyyy-MM").alias("moved_at_label"),
    )
)

with_prev = base.withColumn("prev_moved_at", F.lag("moved_at").over(w_operator))

flagged = (
    with_prev
    .withColumn("starts_session", F.when(F.col("prev_moved_at").isNull() | ((F.unix_timestamp("moved_at") - F.unix_timestamp("prev_moved_at")) / 60 > 60), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("starts_session").over(w_operator_running))

joined = (
    sessionized.alias("s")
    .join(df_warehouses.alias("w"), F.col("w.id") == F.col("s.warehouse_id"), "left")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "left")
    .select(
        "s.*",
        F.col("w.opened_on").alias("warehouse_opened_on"),
        F.col("sk.labels").alias("sku_labels"),
    )
)

with_trends = (
    joined
    .select(
        "*",
        F.count(F.lit(1)).over(w_prev_moved_at_year).alias("rows_in_prev_moved_at_year"),
        F.first("starts_session", ignorenulls=True).over(w_move_type_starts_session_by).alias("first_known_starts_session"),
        F.round(F.col("starts_session") / F.nullif(F.sum("starts_session").over(w_prev_moved_at_year), F.lit(0)), 4).alias("starts_session_share_of_prev_moved_at_year"),
    )
)

result = (
    with_trends
    .filter(F.hour(F.col("prev_moved_at")).between(9, 12))
    .select(
        "id",
        "warehouse_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        "moved_at_label",
        "starts_session",
        "warehouse_opened_on",
        "sku_labels",
        "rows_in_prev_moved_at_year",
        "first_known_starts_session",
        "starts_session_share_of_prev_moved_at_year",
        F.concat_ws(" / ", F.col("move_type"), F.col("moved_at_label")).alias("move_type_moved_at_label_label"),
        F.date_format(F.col("prev_moved_at"), "MMM yyyy").alias("prev_moved_at_label"),
    )
    .orderBy(F.col("moved_at").desc(), "id")
    .limit(5)
)
