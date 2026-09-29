from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region").isin(["South", "East"]) & (F.length("store_name") > 8))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(F.col("opened_month").desc())
)
