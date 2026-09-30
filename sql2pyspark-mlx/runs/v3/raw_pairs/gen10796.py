from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.month("sale_date").isin([4, 9, 10]))
    .select("product_id", "store_id")
    .intersect(
        df_store_sales
        .filter(F.col("units").between(24, 25))
        .select("product_id", "store_id")
    )
    .orderBy(F.col("store_id").desc())
)
