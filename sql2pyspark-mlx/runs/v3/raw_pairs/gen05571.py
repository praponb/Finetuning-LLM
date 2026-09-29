from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2024-09-26")
    .select("sale_date", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id") == 8)
        .select("sale_date", "store_id")
    )
    .orderBy("sale_date")
)
