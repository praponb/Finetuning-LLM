from pyspark.sql import functions as F

prepared = (
    df_warehouses
    .filter(F.upper(F.trim(F.col("wh_code"))).like("B%"))
    .select(
        "id",
        "wh_code",
        "city",
        "opened_on",
        F.length(F.trim(F.col("wh_code"))).alias("wh_code_len"),
    )
)

warehouse_rows = (
    prepared
    .filter((F.year(F.col("opened_on")) == 2025) & (F.col("wh_code_len") >= 6))
    .select(F.col("wh_code_len").alias("key_id"), F.col("id").alias("qty"))
)

move_side = (
    df_stock_moves
    .filter((F.col("move_type").isNotNull()) & (F.col("operator").isNotNull()))
    .select("id", "warehouse_id")
)

result = (
    warehouse_rows
    .union(move_side)
    .distinct()
    .orderBy("qty")
)
