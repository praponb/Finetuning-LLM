from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") <= 1600)
    .select("id", "store_id")
    .subtract(
        df_store_sales
        .filter(F.col("revenue").isNotNull())
        .select("id", "store_id")
    )
)
