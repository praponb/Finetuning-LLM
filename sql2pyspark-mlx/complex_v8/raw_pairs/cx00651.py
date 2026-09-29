from pyspark.sql import functions as F

cleaned_warehouses = (
    df_warehouses
    .filter((F.col("city") == "Singapore") | (F.lower(F.col("wh_code")).like("%ul%")))
    .select(
        "id",
        "wh_code",
        "capacity",
        "opened_on",
        F.datediff(F.lit("2025-12-31").cast("date"), F.col("opened_on")).alias("days_since_opened_on"),
    )
)

warehouse_side = (
    cleaned_warehouses
    .filter((F.col("capacity").between(2614, 11506)) & (F.col("opened_on") >= F.lit("2024-04-13").cast("date")))
    .select(F.col("id").alias("qty"), F.col("wh_code").alias("name"))
)

sku_side = df_skus.select("id", "category")

stacked = warehouse_side.union(sku_side)

result = (
    stacked
    .groupBy("name")
    .agg(F.count("*").alias("n_rows"), F.sum("qty").alias("total_qty"))
    .orderBy("name")
)
