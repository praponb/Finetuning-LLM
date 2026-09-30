from pyspark.sql import functions as F

cleaned = (
    df_stock_moves
    .filter((F.col("warehouse_id").isNotNull()) & (~F.col("operator").isin("op_dew", "op_eve")))
    .select(
        "id",
        "warehouse_id",
        "sku_id",
        "move_type",
        "qty",
        "unit_price",
    )
)

grouped = (
    cleaned
    .groupBy("warehouse_id")
    .agg(
        F.min("qty").alias("min_qty"),
        F.array_sort(F.collect_set("move_type")).alias("move_type_list"),
        F.sum("unit_price").alias("total_unit_price"),
        F.count("*").alias("n_rows"),
    )
)

result = (
    grouped
    .select("warehouse_id", "n_rows", "min_qty")
    .unpivot(
        ids=["warehouse_id"],
        values=["n_rows", "min_qty"],
        variableColumnName="measure",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
    .orderBy("warehouse_id", "measure")
)
