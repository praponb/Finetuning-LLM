from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNotNull())
    .select("amount", F.lit("order").alias("source"))
    .unionByName(
        df_store_sales
        .select(F.col("revenue").alias("amount"), F.lit("store_sale").alias("source"))
    )
    .distinct()
)
