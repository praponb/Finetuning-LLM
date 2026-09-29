from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isNotNull())
    .select(F.col("amount").alias("value"))
    .unionByName(
        df_store_sales
        .filter(F.col("units") == 16)
        .select(F.col("revenue").alias("value"))
    )
    .distinct()
    .orderBy("value")
)
