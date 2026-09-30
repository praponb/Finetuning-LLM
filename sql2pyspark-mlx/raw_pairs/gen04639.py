from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode_outer("coupon_codes").alias("element"))

result = (
    lines
    .groupBy("element")
    .agg(
        F.count(F.when(F.col("element") == "BUNDLE", 1)).alias("bundle_rows"),
        F.count(F.when(F.col("element") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .filter(F.col("bundle_rows") > 1)
)
