from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") >= 23)
    .select(
        "revenue",
        "product_id",
        "units",
        F.date_sub("sale_date", 10).alias("sale_date_minus_10d"),
        F.date_add("sale_date", 85).alias("sale_date_plus_85d"),
    )
)
