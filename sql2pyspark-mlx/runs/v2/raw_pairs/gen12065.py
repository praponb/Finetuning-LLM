from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("units").isNotNull())
    .select("sale_date")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([2, 3, 5]))
        .select("sale_date")
    )
)
