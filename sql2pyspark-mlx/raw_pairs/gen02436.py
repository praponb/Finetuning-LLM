from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") > "2025-07-01")
    .select("sale_date")
    .intersect(
        df_store_sales
        .filter(F.col("units").between(12, 25))
        .select("sale_date")
    )
    .orderBy(F.col("sale_date").desc())
)
