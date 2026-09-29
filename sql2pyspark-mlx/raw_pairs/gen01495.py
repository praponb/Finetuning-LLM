from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "items",
        "attributes",
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.col("attributes")["gift"].alias("gift"),
    )
)
