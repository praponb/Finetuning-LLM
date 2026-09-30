from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([4, 5, 6]))
        .select(F.col("sale_date").alias("opened_date"))
    )
    .orderBy("opened_date")
)
