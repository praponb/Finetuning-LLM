from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id")

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        F.size("items").alias("num_items"),
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.exists("items", lambda li: li["price"] > 100).alias("has_premium_item"),
    )
)

result = (
    parsed
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "has_welcome_code",
        "has_premium_item",
        F.lag("num_items").over(w).alias("prev_num_items"),
        F.lead("num_items").over(w).alias("next_num_items"),
        F.row_number().over(w).alias("event_seq"),
    )
)
