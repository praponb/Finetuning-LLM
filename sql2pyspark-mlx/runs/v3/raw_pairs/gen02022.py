from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id") == 2)
    .select("store_id", "sale_date")
    .intersect(
        df_store_sales
        .filter(F.col("revenue").isNotNull())
        .select("store_id", "sale_date")
    )
)
