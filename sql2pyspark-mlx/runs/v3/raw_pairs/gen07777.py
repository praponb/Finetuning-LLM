from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
    )
    .orderBy(F.col("opened_month").desc(), "avg_sqft")
)
