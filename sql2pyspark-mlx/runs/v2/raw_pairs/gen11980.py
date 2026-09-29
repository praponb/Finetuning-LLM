from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") >= 12)
    .select("product_id")
    .unionByName(
        df_store_sales
        .filter(F.col("units").between(27, 37))
        .select("product_id")
    )
)
