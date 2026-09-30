from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 3]))
    .select("id", "product_id", "sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("revenue").between(1100, 3600))
        .select("id", "product_id", "sale_date")
    )
)
