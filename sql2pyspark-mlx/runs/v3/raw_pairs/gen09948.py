from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("revenue") < 3700)
    .select("sale_date", "store_id")
    .unionByName(
        df_store_sales
        .filter(F.col("sale_date") >= "2024-06-14")
        .select("sale_date", "store_id")
    )
    .distinct()
)
