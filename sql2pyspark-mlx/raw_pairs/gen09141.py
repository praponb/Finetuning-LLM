from pyspark.sql import functions as F

lines = df_carts.select("id", "customer_id", F.explode("coupon_codes").alias("element"))

result = (
    lines
    .groupBy("element")
    .agg(
        F.count(F.when(F.col("element") == "SPRING10", 1)).alias("spring10_rows"),
        F.countDistinct("element").alias("distinct_elements"),
        F.count("*").alias("cnt"),
        F.countDistinct("customer_id").alias("distinct_customer_ids"),
    )
)
