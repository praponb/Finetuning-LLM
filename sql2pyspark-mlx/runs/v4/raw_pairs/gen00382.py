from pyspark.sql import functions as F

result = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        "attributes",
        F.array_max(F.transform("items", lambda li: li["price"])).alias("max_item_price"),
        F.size(F.filter("items", lambda li: li["qty"] >= 3)).alias("bulk_lines"),
    )
)
