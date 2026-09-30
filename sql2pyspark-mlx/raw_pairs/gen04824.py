from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-06-15").cast("date"))
    .select(
        "id",
        "product_id",
        "units",
        (F.col("units") - F.col("revenue")).alias("revenue_per_unit"),
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

result = (
    cleaned
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.price"))
)
