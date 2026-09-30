from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-07-26").cast("date")) & F.col("product_id").isin([4, 9]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.floor("units").alias("units_whole"),
    )
)

result = (
    filtered
    .select("store_id", "id")
    .exceptAll(
        df_store_sales
        .filter(F.year("sale_date") >= 2023)
        .select("store_id", "id")
    )
)
