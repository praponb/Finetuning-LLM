from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status").isNotNull())
    .select("status", F.lit("order").alias("origin"))
    .union(
        df_stores
        .filter(F.col("region").isNotNull())
        .select("region", F.lit("store").alias("origin"))
    )
    .distinct()
)
