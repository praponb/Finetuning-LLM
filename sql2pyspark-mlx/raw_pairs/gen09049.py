from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("store_id", "product_id")
    .exceptAll(
        df_store_sales
        .filter(F.col("sale_date") > "2025-10-01")
        .select("store_id", "product_id")
    )
)
