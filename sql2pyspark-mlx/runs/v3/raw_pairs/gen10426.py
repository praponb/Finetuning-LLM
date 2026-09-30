from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .intersect(
        df_store_sales
        .filter(F.col("store_id").isin([2, 3, 5, 6]))
        .select("sale_date")
    )
)
