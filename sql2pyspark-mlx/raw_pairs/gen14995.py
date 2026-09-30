from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at", "id")
w2 = Window.partitionBy("customer_id").orderBy("max_item_price", "id")

flattened_attrs = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "ship_to",
        "attributes",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "customer_id",
        "max_item_price",
        F.lag("max_item_price", 1).over(w).alias("prev_max_item_price"),
        F.lead("max_item_price", 1).over(w2).alias("next_max_item_price"),
    )
)
