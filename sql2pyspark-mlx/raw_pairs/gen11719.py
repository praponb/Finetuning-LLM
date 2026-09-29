from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("num_attributes", "id")
w2 = Window.orderBy("weight_kg", "id")

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id") == 32)
    .select(
        "id",
        "customer_id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.size("attributes").alias("num_attributes"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.array_max(F.transform("items", lambda i: i["price"])).alias("max_item_price"),
        F.aggregate("items", F.lit(0.0), lambda acc, i: acc + i["qty"] * i["price"], lambda acc: F.round(acc, 2)).alias("items_total"),
    )
)

result = (
    flattened_attrs
    .filter(F.col("weight_kg") >= 3.8)
    .select(
        "id",
        "created_at",
        "num_attributes",
        "weight_kg",
        "items_total",
        F.lag("num_attributes").over(w).alias("prev_num_attributes"),
        F.lead("max_item_price").over(w).alias("next_max_item_price"),
        F.lag("max_item_price").over(w).alias("prev_max_item_price"),
        F.row_number().over(w).alias("event_seq"),
        F.lag("items_total", 2).over(w2).alias("prev_items_total"),
    )
)
