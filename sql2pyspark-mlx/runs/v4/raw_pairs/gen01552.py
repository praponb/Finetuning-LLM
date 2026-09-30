from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode_outer("attributes").alias("key", "attr_value"),
    )
)

result = (
    exploded
    .select("created_at", "key", "attr_value")
    .orderBy(F.col("created_at").desc(), F.col("key").desc())
)
