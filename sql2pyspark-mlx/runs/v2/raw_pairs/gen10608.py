from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") >= "2023-12-01")
    .select("id", "product_id")
    .unionByName(
        df_store_sales
        .filter(F.col("store_id") == 5)
        .select("id", "product_id")
    )
    .orderBy("product_id")
)
