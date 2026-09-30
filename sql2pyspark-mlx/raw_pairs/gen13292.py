from pyspark.sql import functions as F

lines = df_carts.select("id", F.explode_outer("coupon_codes").alias("element"))

result = (
    lines
    .groupBy("element")
    .agg(
        F.countDistinct("element").alias("distinct_elements"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cnt").asc_nulls_last())
)
