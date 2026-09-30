from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("ship_city", "customer_id").orderBy("weight_kg", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("weight_kg", "id")

flattened_attrs = (
    df_carts
    .filter(F.col("customer_id") == 6)
    .select(
        "id",
        "customer_id",
        "attributes",
        F.col("ship_to.city").alias("ship_city"),
        F.col("attributes")["weight_kg"].try_cast("double").alias("weight_kg"),
        F.array_contains("coupon_codes", "WELCOME5").alias("used_welcome5"),
        F.coalesce(F.col("ship_to.zip"), F.lit("UNKNOWN")).alias("zip_or_unknown"),
        F.coalesce(F.col("attributes")["gift"], F.lit("unknown")).alias("gift_or_default"),
    )
)

result = (
    flattened_attrs
    .select(
        "id",
        "customer_id",
        "weight_kg",
        "used_welcome5",
        "zip_or_unknown",
        "gift_or_default",
        F.sum("weight_kg").over(w_running).alias("last7_sum_weight_kg"),
        F.count("weight_kg").over(w_running).alias("last7_count_weight_kg"),
        F.min("weight_kg").over(w_running).alias("last7_min_weight_kg"),
        F.avg("weight_kg").over(w_running).alias("last7_avg_weight_kg"),
        F.lead("weight_kg", 1).over(w2).alias("next_weight_kg"),
    )
)
