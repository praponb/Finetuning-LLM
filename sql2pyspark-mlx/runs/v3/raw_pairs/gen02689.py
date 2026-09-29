from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 3, 5, 10]))
    .select("product_id", "units")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") >= "2023-11-24")
        .select("product_id", "units")
    )
)
