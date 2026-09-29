from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") == 32)
    .select("store_id", "sale_date")
    .subtract(
        df_store_sales
        .filter(F.col("product_id") == 8)
        .select("store_id", "sale_date")
    )
)
