from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-07-12").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        F.col("units").cast("string").alias("units_str"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .select("src.id", "src.store_id", "src.product_id", F.col("p.sqft"))
)
