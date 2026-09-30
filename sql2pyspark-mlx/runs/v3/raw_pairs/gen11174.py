from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 6]))
    .select("product_id", "sale_date")
    .intersect(
        df_store_sales
        .filter(F.col("sale_date") > "2023-05-13")
        .select("product_id", "sale_date")
    )
    .orderBy("product_id")
)
