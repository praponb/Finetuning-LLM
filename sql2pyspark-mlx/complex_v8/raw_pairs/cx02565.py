from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_dedup = Window.partitionBy("move_type", "operator_group").orderBy(F.col("unit_price").desc(), F.col("id").desc())

cleaned = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) | (F.month(F.col("moved_at")).isin(1, 2)))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "moved_at",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_eve", "op_bee"), "group_a").when(F.col("operator") == "op_ann", "group_b").otherwise("other").alias("operator_group"),
        F.length(F.trim(F.col("move_type"))).alias("move_type_len"),
        F.initcap(F.trim(F.col("move_type"))).alias("move_type_clean"),
    )
)

with_activity = (
    cleaned
    .crossJoin(cleaned.agg(F.avg("unit_price").alias("overall_avg_unit_price")))
    .filter(F.col("unit_price") > F.col("overall_avg_unit_price"))
    .drop("overall_avg_unit_price")
)

latest_per_key = (
    with_activity
    .withColumn("rn", F.row_number().over(w_dedup))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest_per_key
    .select(
        "id",
        F.col("qty").alias("qty_v"),
        F.col("move_type_len").alias("move_type_len_v"),
    )
    .unpivot(
        ids=["id"],
        values=["qty_v", "move_type_len_v"],
        variableColumnName="metric",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
