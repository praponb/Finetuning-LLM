from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([2, 25, 28]))
    .select(
        "id",
        "discount",
        "customer_id",
        F.when(F.col("channel") == "mobile", 1).otherwise(0).alias("is_mobile"),
    )
    .orderBy(F.col("id").desc())
    .limit(50)
)
