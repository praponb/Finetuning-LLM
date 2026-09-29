from pyspark.sql import functions as F

lines = (
    df_carts
    .select(
        "id",
        "customer_id",
        F.explode("attributes").alias("attr_key", "attr_value"),
    )
)

result = (
    lines
    .select("id", "customer_id", "attr_key")
    .orderBy("id")
)
