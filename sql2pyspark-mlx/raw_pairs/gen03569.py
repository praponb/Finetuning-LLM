from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("opened_date").isNotNull())
    .select("opened_date", F.lit("store").alias("source"))
    .union(
        df_orders
        .filter(F.col("order_date").isNotNull())
        .select("order_date", F.lit("order").alias("source"))
    )
    .distinct()
    .orderBy(F.col("opened_date").desc())
)
