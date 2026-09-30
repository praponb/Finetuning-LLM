from pyspark.sql import functions as F

result = (
    df_store_sales
    .select("store_id")
    .intersectAll(
        df_store_sales
        .filter(F.col("sale_date") >= "2024-07-02")
        .select("store_id")
    )
)
