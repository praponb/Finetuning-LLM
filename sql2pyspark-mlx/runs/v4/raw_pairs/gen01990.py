from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 2)
    .select("units", "product_id")
    .unionByName(
        df_store_sales
        .filter(F.col("revenue").between(1700, 3900))
        .select("units", "product_id")
    )
    .distinct()
    .orderBy("units")
)
