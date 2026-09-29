from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id").orderBy("weight_kg", "id")

parsed = (
    df_carts
    .select(
        "id",
        "customer_id",
        "items",
        "ship_to",
        F.col("attributes")["utm_source"].alias("utm_source"),
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.col("ship_to.zip").alias("ship_zip"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
    )
)

result = (
    parsed
    .filter(F.col("has_premium_item"))
    .select(
        "id",
        "customer_id",
        "utm_source",
        "has_premium_item",
        "weight_kg",
        F.lag("weight_kg").over(w_ordered).alias("prev_weight_kg"),
        F.lead("weight_kg").over(w_ordered).alias("next_weight_kg"),
        F.row_number().over(w_ordered).alias("seq_no"),
    )
)
