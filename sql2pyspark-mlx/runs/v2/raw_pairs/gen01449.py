from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
        "city",
        "region",
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.avg("sqft").alias("avg_sqft"),
    )
    .orderBy("region_values")
)
