from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([3, 5, 6]))
    .select("sale_date")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date") >= "2023-07-14")
        .select("sale_date")
    )
    .distinct()
)
