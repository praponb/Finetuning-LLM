from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 3100)
    .select("id", "store_id")
    .intersect(
        df_store_sales
        .filter(F.col("sale_date") < "2023-09-15")
        .select("id", "store_id")
    )
)
