from pyspark.sql import functions as F

valid = (
    df_stores
    .filter(F.col("opened_date") >= F.lit("2023-11-14").cast("date"))
    .select("*", F.concat("store_name", F.lit(" "), "city").alias("store_name_city"))
)

result = (
    valid
    .groupBy("region")
    .pivot("city", ["Bangkok", "Chiang Mai"])
    .agg(F.avg("sqft").alias("amount"), F.count(F.lit(1)).alias("cnt"))
    .select(
        "region",
        F.col("Bangkok_amount").alias("bangkok_amount"),
        F.col("Bangkok_cnt").alias("bangkok_cnt"),
        F.col("Chiang Mai_amount").alias("chiang_mai_amount"),
        F.col("Chiang Mai_cnt").alias("chiang_mai_cnt"),
    )
)
