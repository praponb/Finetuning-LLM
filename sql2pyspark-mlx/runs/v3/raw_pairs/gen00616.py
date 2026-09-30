from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"), "city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.sum("sqft").alias("total_sqft"),
    )
    .filter(F.col("total_sqft") >= 92000)
    .orderBy("opened_month")
)
