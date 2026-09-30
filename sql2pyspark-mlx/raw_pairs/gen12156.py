from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "ship_to",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.size(F.filter("items", lambda item: item["qty"] >= 3)).alias("bulk_lines"),
        F.col("attributes")["gift"].alias("gift"),
        F.array_max(F.transform("items", lambda item: item["price"])).alias("max_item_price"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
    )
)
