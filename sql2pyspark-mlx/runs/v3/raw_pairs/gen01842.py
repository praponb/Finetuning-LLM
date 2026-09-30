from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.countDistinct("sqft").alias("unique_regions"),
        F.countDistinct("sqft").alias("unique_citys"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
    .orderBy("opened_month")
)
