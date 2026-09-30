from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2023-10-12")
    .select("units")
    .unionByName(
        df_store_sales
        .filter(F.month("sale_date").isin([2, 3, 5]))
        .select("units")
    )
    .orderBy("units")
)
