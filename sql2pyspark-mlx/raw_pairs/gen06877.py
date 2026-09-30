from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("key", "v"),
    )
)

result = (
    lines
    .select("customer_id", "key", "v")
    .orderBy(F.col("customer_id").desc())
)
