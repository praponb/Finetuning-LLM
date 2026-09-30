from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_operator_by_unit_price_prior = Window.partitionBy("operator").orderBy(F.col("unit_price").desc(), "id").rowsBetween(-1, -1)
w_operator_by_unit_price_full = Window.partitionBy("operator").orderBy(F.col("unit_price").desc(), "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w_move_type = Window.partitionBy("move_type")

prepared_moves = (
    df_stock_moves
    .filter((F.col("warehouse_id") <= 3) & (F.col("moved_at") < F.lit("2025-02-12 13:00:00").cast("timestamp")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.date_format(F.col("moved_at"), "MMM yyyy").alias("moved_at_label"),
        F.upper(F.trim(F.col("operator"))).alias("operator_clean"),
    )
)

screened = (
    prepared_moves
    .crossJoin(prepared_moves.agg(F.max("unit_price").alias("overall_max_unit_price")))
    .filter(F.col("unit_price") < F.col("overall_max_unit_price"))
    .drop("overall_max_unit_price")
)

without_matches = (
    screened.alias("s")
    .join(df_stock_moves.alias("st"), (F.col("st.sku_id") == F.col("s.sku_id")) & (F.col("st.move_type").isin("TRANSFER", "ADJUST", "INBOUND")) & (F.col("st.warehouse_id") >= 2), "left_anti")
)

with_trends = (
    without_matches
    .select(
        "*",
        F.sum("unit_price").over(w_operator_by_unit_price_prior).alias("unit_price_sum_1_prior"),
        F.nth_value("unit_price", 2).over(w_operator_by_unit_price_full).alias("second_unit_price"),
        F.max("unit_price").over(w_move_type).alias("move_type_max_unit_price"),
    )
)

result = (
    with_trends
    .cube("warehouse_id", "operator", "sku_id")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("unit_price_sum_1_prior"), 2).alias("stddev_unit_price_sum_1_prior"),
    )
    .orderBy(
        F.col("warehouse_id").asc_nulls_last(),
        F.col("operator").asc_nulls_last(),
        F.col("sku_id").asc_nulls_last(),
    )
)
