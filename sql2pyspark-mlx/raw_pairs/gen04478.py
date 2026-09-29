from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("created_at", "id")

unpacked = (
    df_carts
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "created_at",
        "ship_to",
        F.exists("items", lambda x: x["price"] > 100).alias("has_premium_item"),
        F.coalesce(F.col("attributes").getItem("gift"), F.lit("direct")).alias("gift_or_default"),
        F.coalesce(F.col("attributes")["utm_source"], F.lit("unknown")).alias("utm_source_or_default"),
        F.col("attributes").getItem("weight_kg").try_cast("double").alias("weight_kg"),
    )
)

result = (
    unpacked
    .select(
        "*",
        F.lead("weight_kg").over(w).alias("next_weight_kg"),
        F.lag("weight_kg").over(w).alias("prev_weight_kg"),
        F.row_number().over(w).alias("seq_no"),
    )
)
