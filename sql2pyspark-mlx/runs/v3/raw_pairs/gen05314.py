from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units") != 36)
    .select("id", "units")
    .unionByName(
        df_store_sales
        .filter(F.col("product_id").isin([4, 7]))
        .select("id", "units")
    )
    .distinct()
)
