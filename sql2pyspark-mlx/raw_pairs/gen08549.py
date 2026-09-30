from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("tag"))

result = (
    exploded
    .select("id", "created_at", "tag")
    .orderBy(F.col("created_at").asc_nulls_last())
)
