from pyspark.sql import functions as F

result = (
    df_orders
    .select(
        "id",
        "customer_id",
        "store_id",
        "discount",
        F.substring("status", 1, 2).alias("status_prefix"),
    )
    .orderBy(F.col("store_id").desc())
)
