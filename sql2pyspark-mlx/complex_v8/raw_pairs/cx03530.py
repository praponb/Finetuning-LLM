from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_warehouse_id = Window.partitionBy("warehouse_id").orderBy("moved_at", "id")
w_warehouse_id_running = Window.partitionBy("warehouse_id").orderBy("moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_move_type_session_no_by_prev = Window.partitionBy("move_type", "session_no").orderBy("prev_moved_at", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_session_no_by_prev_2 = Window.partitionBy("move_type", "session_no").orderBy("prev_moved_at", "id")
w_warehouse_id_move_type_by_pr = Window.partitionBy("warehouse_id", "move_type").orderBy(F.col("prev_moved_at").asc_nulls_last(), "id")

valid = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
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

with_prev = valid.withColumn("prev_moved_at", F.lag("moved_at").over(w_warehouse_id))

flagged = (
    with_prev
    .withColumn("new_session_flag", F.when(F.col("prev_moved_at").isNull() | (F.col("moved_at") > F.col("prev_moved_at") + F.expr("INTERVAL 60 MINUTES")), 1).otherwise(0))
)

sessionized = flagged.withColumn("session_no", F.sum("new_session_flag").over(w_warehouse_id_running))

joined_sku = (
    sessionized.alias("s")
    .join(df_skus.alias("sk"), F.col("sk.id") == F.col("s.sku_id"), "inner")
    .select(
        "s.id",
        "s.warehouse_id",
        "s.sku_id",
        "s.move_type",
        "s.qty",
        "s.operator",
        "s.unit_price",
        "s.prev_moved_at",
        "s.new_session_flag",
        "s.session_no",
        F.col("sk.labels").alias("sku_labels"),
    )
)

sequenced = (
    joined_sku
    .select(
        "id",
        "warehouse_id",
        "qty",
        "operator",
        "session_no",
        F.first("unit_price").over(w_move_type_session_no_by_prev).alias("first_unit_price"),
        F.row_number().over(w_move_type_session_no_by_prev_2).alias("prev_moved_at_seq"),
        F.ntile(3).over(w_warehouse_id_move_type_by_pr).alias("prev_moved_at_tercile"),
    )
)

result = (
    sequenced
    .filter(~F.col("operator").isin("op_dew", "op_eve"))
    .select("prev_moved_at_tercile")
    .distinct()
)
