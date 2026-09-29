from pyspark.sql import functions as F

result = (
    df_carts
    .filter(F.col("created_at") >= "2023-05-01")
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        "attributes",
        F.size("attributes").alias("num_attributes"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.col("ship_to.zip").alias("ship_zip"),
    )
)
