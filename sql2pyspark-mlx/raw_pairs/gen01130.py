from pyspark.sql import functions as F

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-05-03"))) & F.col("product_id").isin([4, 8, 9, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        F.ceil("revenue").alias("revenue_whole"),
    )
)

result = (
    base
    .select("product_id")
    .intersect(
        df_store_sales
        .filter(F.year("sale_date") >= 2024)
        .select("product_id")
    )
)
