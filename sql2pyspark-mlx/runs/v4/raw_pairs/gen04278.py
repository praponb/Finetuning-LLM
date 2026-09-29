from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "ship_city").orderBy("weight_kg", "id")
w2 = Window.partitionBy("ship_city").orderBy("weight_kg", "id")
w3 = Window.partitionBy("ship_city").orderBy(F.col("weight_kg").desc(), "id")

flattened_attrs = (
    df_carts
    .filter(F.col("created_at") < "2023-03-31")
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "attributes",
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.transform(
            F.filter("items", lambda li: li["promo"].isNotNull()),
            lambda li: F.struct(li["product_id"].alias("pid"), F.round(li["price"] * 0.9, 2).alias("discounted_price")),
        ).alias("promo_lines"),
        F.col("attributes").getItem("utm_source").alias("utm_source"),
        F.forall("items", lambda li: li["qty"] <= 5).alias("all_small_qty"),
        F.col("ship_to.city").alias("ship_city"),
    )
)

result = (
    flattened_attrs
    .select(
        "*",
        F.lead("weight_kg", 1).over(w).alias("next_weight_kg"),
        F.lag("weight_kg", 1, 0).over(w2).alias("prev_weight_kg"),
        F.ntile(3).over(w3).alias("weight_kg_bucket"),
    )
)
