from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "created_at",
        "ship_to",
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.exists("items", lambda item: item["price"] > 100).alias("has_premium_item"),
    )
)
