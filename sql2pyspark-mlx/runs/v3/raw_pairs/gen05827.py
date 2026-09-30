from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue").between(2900, 3000))
    .select("sale_date", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id").isin([3, 5, 8]))
        .select("sale_date", "store_id")
    )
)
