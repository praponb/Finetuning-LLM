from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("product_id") == 5)
    .select("id", "units", "store_id")
    .intersect(
        df_store_sales
        .filter(F.col("revenue").isNotNull())
        .select("id", "units", "store_id")
    )
    .orderBy(F.col("units").desc())
)
