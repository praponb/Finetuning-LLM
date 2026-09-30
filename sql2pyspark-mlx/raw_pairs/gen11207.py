from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        F.size("items").alias("num_items"),
        F.col("attributes")["gift"].alias("gift"),
        F.coalesce(F.col("attributes")["device"], F.lit("none")).alias("device_or_default"),
        F.size("attributes").alias("num_attributes"),
    )
)
