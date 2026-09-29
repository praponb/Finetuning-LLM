from pyspark.sql import functions as F

result = (
    df_orders
    .select("order_date", F.lit("order").alias("source"))
    .unionByName(
        df_stores
        .select(F.col("opened_date").alias("order_date"), F.lit("store").alias("source"))
    )
    .distinct()
)
