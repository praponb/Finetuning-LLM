from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("store_name").like("%Down%") & F.col("opened_date").between("2024-02-13", "2024-05-17") & (F.lower(F.trim("store_name")) == "riverside"))
)

agg = (
    base
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"))
)

result = (
    agg
    .orderBy("city_values", "opened_month")
    .limit(10)
)
