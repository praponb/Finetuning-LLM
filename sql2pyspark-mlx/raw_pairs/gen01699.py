from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("num_items", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy(F.col("weight_kg").desc(), "id")
w3 = Window.orderBy("weight_kg", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

parsed = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "items",
        "attributes",
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.size("items").alias("num_items"),
        F.transform("items", lambda i: i["product_id"]).alias("product_ids"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
    )
)

result = (
    parsed
    .filter(F.col("num_items").isNull())
    .select(
        "id",
        "created_at",
        "coupon_codes",
        "has_welcome_code",
        "weight_kg",
        F.sum("num_items").over(w).alias("cumulative_num_items"),
        F.ntile(5).over(w2).alias("weight_kg_bucket"),
        F.sum("weight_kg").over(w3).alias("cumulative_weight_kg"),
    )
)
