from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "attributes",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
    )
)
