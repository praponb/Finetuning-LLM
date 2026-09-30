from pyspark.sql import functions as F

flattened = df_carts.select("id", F.explode_outer("coupon_codes").alias("value"))

result = (
    flattened
    .groupBy("value")
    .agg(
        F.count(F.when(F.col("value") == "SPRING10", 1)).alias("spring10_rows"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("value") == "WELCOME5", 1)).alias("welcome5_rows"),
    )
)
