from pyspark.sql import functions as F

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = exploded.select("id", "customer_id", "created_at", "attr_key")
