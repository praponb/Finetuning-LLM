from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("units") == 31)
    .withColumn("sale_date_minus_41d", F.date_sub("sale_date", 41))
)

result = (
    cleaned
    .select("id", "product_id")
    .subtract(
        df_store_sales
        .filter(F.col("sale_date") < "2025-10-01")
        .select("id", "product_id")
    )
)
