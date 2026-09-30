from pyspark.sql import functions as F

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("element"))

result = (
    flattened
    .select("created_at", "element")
    .orderBy("element", "created_at")
)
