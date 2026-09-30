from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode_outer("coupon_codes").alias("code"))

result = (
    lines
    .select("id", "code")
    .orderBy(F.col("code").asc_nulls_first())
)
