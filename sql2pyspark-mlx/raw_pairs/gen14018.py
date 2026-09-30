from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("key", "v"),
    )
)

result = (
    exploded
    .select("id", "key", "v")
    .orderBy(F.col("id").asc_nulls_last())
)
