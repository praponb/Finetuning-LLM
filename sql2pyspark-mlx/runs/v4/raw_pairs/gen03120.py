from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter((F.col("units") != 25) | F.col("product_id").isin([1, 2, 9]))
    .select(
        "revenue",
        "product_id",
        "units",
        F.date_add("sale_date", 50).alias("sale_date_plus_50d"),
    )
    .orderBy(F.col("units").desc())
)
