from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 5]))
    .select("product_id")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date") > "2024-05-01")
        .select("product_id")
    )
    .distinct()
)
