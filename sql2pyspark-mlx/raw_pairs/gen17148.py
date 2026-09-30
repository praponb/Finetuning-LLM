from pyspark.sql import functions as F

result = (
    df_stores
    .filter(~F.col("city").isin(["Chiang Mai", "London", "Singapore"]) & (F.month("opened_date").isin([2, 8, 12]) | F.col("city").isin(["New York", "Chiang Mai"])))
    .groupBy(F.month("opened_date").alias("opened_month"), "city")
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
    .orderBy("city", F.col("opened_month").desc())
)
