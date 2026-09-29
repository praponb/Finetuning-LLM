from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-08-24").cast("date"))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.floor("units").alias("units_whole"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

result = (
    valid
    .select("product_id", "id")
    .exceptAll(
        df_store_sales
        .filter(F.col("product_id") == 3)
        .select("product_id", "id")
    )
)
