from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("tag"))

result = (
    lines
    .groupBy("customer_id")
    .agg(
        F.count("*").alias("cnt"),
        F.count(F.when(F.col("tag") == "SPRING10", 1)).alias("spring10_rows"),
        F.count(F.when(F.col("tag") == "WELCOME5", 1)).alias("welcome5_count"),
        F.size(F.collect_set("tag")).alias("num_tags"),
    )
    .filter(F.col("welcome5_count") > 1)
    .orderBy("spring10_rows")
)
