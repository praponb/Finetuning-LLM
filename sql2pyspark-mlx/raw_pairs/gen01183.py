from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .pivot("city", ["New York", "Berlin"])
    .agg(F.max("credit_limit").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "country",
        F.col("New York_amount").alias("new_york_amount"),
        F.col("New York_cnt").alias("new_york_cnt"),
        F.col("Berlin_amount").alias("berlin_amount"),
        F.col("Berlin_cnt").alias("berlin_cnt"),
    )
)
