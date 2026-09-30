from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_distinct_operators_n_operato = Window.partitionBy("distinct_operators", "n_operator_op_eve").orderBy(F.col("total_unit_price").desc(), "move_type").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type_by_row_count = Window.partitionBy("move_type").orderBy(F.col("row_count").desc_nulls_first(), "move_type")

prepared = (
    df_stock_moves
    .filter((F.hour(F.col("moved_at")).between(10, 16)) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "moved_at",
        "operator",
        "unit_price",
        F.to_date(F.col("moved_at")).alias("moved_at_date"),
        F.date_format(F.col("moved_at"), "yyyy-MM-dd HH:00").alias("moved_at_label"),
        F.least(F.col("unit_price"), F.col("qty")).alias("least_unit_price_qty"),
    )
)

grouped = (
    prepared
    .filter(F.col("warehouse_id").isNotNull())
    .groupBy("move_type")
    .agg(
        F.sum(F.when(F.col("operator") == "op_chan", 1).otherwise(0)).alias("n_operator_op_chan"),
        F.count("*").alias("row_count"),
        F.sum(F.when(F.col("operator") == "op_eve", 1).otherwise(0)).alias("n_operator_op_eve"),
        F.countDistinct("operator").alias("distinct_operators"),
        F.round(F.sum("unit_price"), 2).alias("total_unit_price"),
    )
    .filter(F.col("row_count") >= 2)
)

scored = (
    grouped
    .select(
        "*",
        F.lower(F.regexp_replace(F.trim(F.col("move_type")), " ", "_")).alias("move_type_slug"),
    )
)

with_activity = (
    scored
    .crossJoin(scored.agg(F.avg("total_unit_price").alias("overall_avg_total_unit_price")))
    .filter(F.col("total_unit_price") > F.col("overall_avg_total_unit_price"))
    .drop("overall_avg_total_unit_price")
)

with_windows = (
    with_activity
    .select(
        "move_type",
        "n_operator_op_chan",
        "row_count",
        "n_operator_op_eve",
        "distinct_operators",
        "total_unit_price",
        "move_type_slug",
        F.first("distinct_operators").over(w_distinct_operators_n_operato).alias("first_distinct_operators"),
        F.lead("distinct_operators", 2).over(w_move_type_by_row_count).alias("next2_distinct_operators"),
    )
)

result = (
    with_windows
    .filter(F.col("move_type") != "TRANSFER")
    .select(
        "move_type",
        "n_operator_op_chan",
        "row_count",
        "n_operator_op_eve",
        "move_type_slug",
    )
    .orderBy(F.col("move_type_slug").desc())
)
