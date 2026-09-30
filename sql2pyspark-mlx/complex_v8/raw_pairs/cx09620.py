from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_move_type_len_operator_by_qt = Window.partitionBy("move_type_len", "operator").orderBy("qty", "id")
w_sku_id = Window.partitionBy("sku_id")
w_move_type_len_operator_by_qt_2 = Window.partitionBy("move_type_len", "operator").orderBy("qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_latest = Window.partitionBy("operator").orderBy(F.col("qty_percent_rank").desc(), F.col("id").desc())

scoped = (
    df_stock_moves
    .filter((F.col("unit_price") >= 61.2) & (F.col("warehouse_id").isNotNull()))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        F.concat_ws(" / ", F.col("operator"), F.col("move_type")).alias("operator_move_type_label"),
    )
)

derived = (
    scoped
    .filter(F.col("move_type") != "ADJUST")
    .select(
        "id",
        "sku_id",
        "qty",
        "operator",
        "operator_move_type_label",
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.lower(F.trim(F.col("operator_move_type_label"))).alias("operator_move_type_label_clean"),
    )
)

operator_stats = (
    derived
    .groupBy("operator")
    .agg(F.avg("move_type_len").alias("avg_move_type_len"))
)

vs_group = (
    derived.alias("s")
    .join(operator_stats.alias("g"), F.col("g.operator") == F.col("s.operator"), "inner")
    .filter(F.col("s.move_type_len") > F.col("g.avg_move_type_len"))
    .select(
        "s.*",
        "g.avg_move_type_len",
        F.round(F.col("s.move_type_len") - F.col("g.avg_move_type_len"), 2).alias("move_type_len_minus_avg"),
    )
)

ranked = (
    vs_group
    .select(
        "id",
        "operator",
        "operator_move_type_label",
        "move_type_len",
        "operator_move_type_label_clean",
        "avg_move_type_len",
        "move_type_len_minus_avg",
        F.round(F.percent_rank().over(w_move_type_len_operator_by_qt), 3).alias("qty_percent_rank"),
        F.size(F.collect_set("operator").over(w_sku_id)).alias("distinct_operator_in_sku_id"),
        F.count(F.lit(1)).over(w_sku_id).alias("rows_in_sku_id"),
        F.count(F.lit(1)).over(w_move_type_len_operator_by_qt_2).alias("running_count"),
    )
)

latest = (
    ranked
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

derived_2 = (
    latest
    .select(
        "id",
        "operator_move_type_label",
        "move_type_len",
        "avg_move_type_len",
        "move_type_len_minus_avg",
        "qty_percent_rank",
        "distinct_operator_in_sku_id",
        F.upper(F.trim(F.col("operator_move_type_label"))).alias("operator_move_type_label_clean_2"),
        F.coalesce(F.col("running_count"), F.lit(0)).alias("running_count_filled"),
    )
)

result = (
    derived_2
    .select("id", "move_type_len_minus_avg", "qty_percent_rank")
    .unpivot(
        ids=["id"],
        values=["move_type_len_minus_avg", "qty_percent_rank"],
        variableColumnName="field",
        valueColumnName="amount",
    )
    .filter(F.col("amount").isNotNull())
)
