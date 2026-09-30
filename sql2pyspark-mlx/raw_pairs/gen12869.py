from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 1)
    .select("units")
    .unionByName(
        df_store_sales
        .filter(F.month("sale_date").isin([4, 6, 11]))
        .select("units")
    )
)
