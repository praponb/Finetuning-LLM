from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([2, 5, 6]))
    .select("product_id", "sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date") < "2024-09-16")
        .select("product_id", "sale_date")
    )
    .distinct()
    .orderBy("product_id")
)
