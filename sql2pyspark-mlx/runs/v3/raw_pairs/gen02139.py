from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").between(18, 30))
    .select("product_id")
    .unionByName(
        df_store_sales
        .filter(F.year("sale_date") == 2024)
        .select("product_id")
    )
)
