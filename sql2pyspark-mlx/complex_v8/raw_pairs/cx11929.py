from pyspark.sql import functions as F

cleaned = (
    df_warehouses
    .filter((F.upper(F.trim(F.col("wh_code"))).like("B%")) & (F.col("opened_on") < F.lit("2025-02-14").cast("date")))
    .select(
        "id",
        "wh_code",
        "city",
        "capacity",
        "opened_on",
        F.concat(F.col("wh_code"), F.lit("#"), F.col("id").cast("string")).alias("wh_code_ref"),
    )
)

warehouse_rows = (
    cleaned
    .filter((F.col("capacity") >= 2614) & (F.col("wh_code_ref").isNotNull()))
    .select("id")
)

sku_side = df_skus.select("id")

result = warehouse_rows.subtract(sku_side)
