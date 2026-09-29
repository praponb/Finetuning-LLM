from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("id", "product_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("product_id").isin([4, 5, 7, 9]))
        .select("id", "product_id")
    )
)
