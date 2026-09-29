from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_latest = Window.partitionBy("move_type").orderBy("moved_at_plus_2h", "id")

recent = (
    df_stock_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "operator",
        (F.col("moved_at") + F.expr("INTERVAL 2 HOURS")).alias("moved_at_plus_2h"),
        F.quarter(F.col("moved_at")).alias("moved_at_quarter"),
    )
)

scored = recent.select("*", F.lower(F.trim(F.col("move_type"))).alias("move_type_clean"))

unique_rows = (
    scored
    .withColumn("rn", F.row_number().over(w_latest))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

per_group = (
    unique_rows
    .groupBy(F.year("moved_at_plus_2h").alias("moved_at_plus_2h_year"), "move_type")
    .agg(
        F.percentile_approx("moved_at_quarter", 0.5).alias("median_moved_at_quarter"),
        F.count("*").alias("cnt"),
        F.round(F.stddev("qty"), 2).alias("stddev_qty"),
        F.sum(F.when(F.col("operator").isNotNull(), 1).otherwise(0)).alias("n_operator_is_not_null"),
        F.countDistinct("move_type").alias("distinct_move_types"),
        F.max("qty").alias("max_qty_check"),
    )
    .filter(F.col("max_qty_check") > 137)
    .drop("max_qty_check")
)

result = (
    per_group
    .select(
        "moved_at_plus_2h_year",
        "move_type",
        F.col("distinct_move_types").alias("m_distinct_move_types"),
        F.col("n_operator_is_not_null").alias("n_operator_is_not_null_v"),
        F.col("median_moved_at_quarter").alias("m_median_moved_at_quarter"),
    )
    .unpivot(
        ids=["moved_at_plus_2h_year", "move_type"],
        values=["m_distinct_move_types", "n_operator_is_not_null_v", "m_median_moved_at_quarter"],
        variableColumnName="field",
        valueColumnName="value",
    )
    .orderBy("moved_at_plus_2h_year", "move_type", "field")
)
