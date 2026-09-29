from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode("coupon_codes").alias("tag"))

result = (
    flattened
    .select("id", "tag")
    .orderBy(F.col("id").desc_nulls_first(), F.col("tag").desc())
)
