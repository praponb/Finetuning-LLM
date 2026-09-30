from pyspark.sql import functions as F

scoped_moves = (
    df_stock_moves
    .filter(F.col("warehouse_id").isNotNull())
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        F.when(F.col("operator").isin("op_chan", "op_dew"), "group_a").when(F.col("operator") == "op_eve", "group_b").otherwise("other").alias("operator_group"),
    )
)

result = (
    scoped_moves
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "operator",
        "unit_price",
        "operator_group",
        F.length(F.trim(F.col("operator"))).alias("operator_len"),
        F.lower(F.trim(F.col("operator_group"))).alias("operator_group_clean"),
    )
    .orderBy(F.col("move_type").desc(), F.col("sku_id").desc())
)
