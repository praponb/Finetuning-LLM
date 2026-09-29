from pyspark.sql import functions as F

base = df_stores.filter(F.col("region") == "Central")

agg = (
    base
    .groupBy("city")
    .agg(
        F.max("opened_date").alias("latest_opened_date"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
)

result = (
    agg
    .select("city", "latest_opened_date", "city_list")
    .orderBy(F.col("city").desc(), "latest_opened_date")
    .limit(50)
)
