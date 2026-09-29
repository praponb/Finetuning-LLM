from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("%Sia%") & (F.col("store_name").like("%Centra%") | (F.length("store_name") > 14)))
    .groupBy(
        F.when(F.col("city").isin(["Berlin", "Bangkok"]), "priority").otherwise("other").alias("city_group"),
    )
    .agg(
        F.count("*").alias("store_count"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .filter(F.col("store_count") >= 3)
    .orderBy("last_opened_date", "city_group")
    .limit(5)
)
