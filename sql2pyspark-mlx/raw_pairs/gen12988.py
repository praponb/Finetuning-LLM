from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode_outer("coupon_codes").alias("code"))

result = (
    lines
    .groupBy("code")
    .agg(
        F.count_if(F.col("code") == "WELCOME5").alias("welcome5_events"),
        F.count("*").alias("row_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("code")).alias("num_codes"),
        F.count(F.when(F.col("code") == "SPRING10", 1)).alias("spring10_rows"),
    )
    .filter(F.col("spring10_rows") >= 1)
)
