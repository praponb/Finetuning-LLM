from pyspark.sql import functions as F

valid_stores = (
    df_stores
    .filter((F.col("region") == "Central") & F.col("region").isin(["East", "North"]))
)

agg = (
    valid_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.sort_array(F.collect_set("city")).alias("city_list"))
)

result = (
    agg
    .select("opened_month", "city_list")
    .orderBy(F.col("opened_month").desc())
)
