from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") >= 22)
    .select("sale_date")
    .intersect(
        df_store_sales
        .filter(F.col("sale_date") > "2023-04-01")
        .select("sale_date")
    )
)
