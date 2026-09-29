from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 2200)
    .select("id", "product_id", "units")
    .unionByName(
        df_store_sales
        .filter(F.col("units") <= 12)
        .select("id", "product_id", "units")
    )
)
