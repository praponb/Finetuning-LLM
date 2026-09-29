from pyspark.sql import functions as F

base = (
    df_warehouses
    .filter((F.col("opened_on") < F.lit("2025-07-03").cast("date")) & (F.col("capacity") >= 2614))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.date_format(F.col("opened_on"), "yyyy-MM").alias("opened_on_label"),
    )
)

warehouse_side = (
    base
    .filter(F.length(F.trim(F.col("wh_code"))) >= 4)
    .select("id", "city", F.lit("warehouse").alias("src"))
)

move_side = (
    df_stock_moves
    .select(
        F.col("sku_id").alias("id"),
        F.col("operator").alias("city"),
        F.lit("move").alias("src"),
    )
)

sku_rows = (
    df_skus
    .filter(F.lower(F.col("sku_code")).like("%ap%"))
    .select("id", F.col("category").alias("city"), F.lit("sku").alias("src"))
)

result = (
    warehouse_side
    .unionByName(move_side)
    .unionByName(sku_rows)
    .orderBy("city")
)
