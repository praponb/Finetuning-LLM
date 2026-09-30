from pyspark.sql import functions as F

result = (
    df_store_sales
    .select(F.col("revenue").alias("value"), F.lit("store_sale").alias("source"))
    .unionByName(
        df_orders
        .select(F.col("amount").alias("value"), F.lit("order").alias("source"))
    )
)
