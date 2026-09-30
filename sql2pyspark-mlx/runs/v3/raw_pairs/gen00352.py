from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("sale_date").alias("event_date"))
    .unionByName(
        df_stores
        .filter(F.col("opened_date") > "2023-05-01")
        .select(F.col("opened_date").alias("event_date"))
    )
)
