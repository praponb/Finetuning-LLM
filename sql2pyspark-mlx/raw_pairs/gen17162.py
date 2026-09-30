from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(21, 31))
    .select("sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date").between("2025-01-06", "2025-03-05"))
        .select("sale_date")
    )
    .distinct()
)
