from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "ship_city").orderBy("created_at", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("ship_city").orderBy("created_at", "id")

unpacked = (
    df_carts
    .filter(F.col("created_at") < "2023-06-05")
    .select(
        "id",
        "customer_id",
        "created_at",
        "items",
        "attributes",
        F.array_contains(F.map_keys("attributes"), "weight_kg").alias("has_weight_kg"),
        F.coalesce(F.col("attributes")["gift"], F.lit("unknown")).alias("gift_or_default"),
        F.col("ship_to.city").alias("ship_city"),
        F.exists("coupon_codes", lambda c: c.like("W%")).alias("has_welcome_code"),
        F.col("attributes")["utm_source"].alias("utm_source"),
    )
)

result = (
    unpacked
    .withColumn("last_known_ship_city", F.last("ship_city", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)
