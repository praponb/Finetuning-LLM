from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") <= 37)
    .select("id", "product_id", "units")
    .unionByName(
        df_store_sales
        .filter(F.col("units") > 30)
        .select("id", "product_id", "units")
    )
)
