from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("city"))).alias("city_values"),
        F.round(((100 * F.sum(F.when(F.col("region") == "South", 1).otherwise(0))) / F.count("*")), 1).alias("pct_south"),
        F.sum(F.when(F.col("city") == "New York", F.col("sqft")).otherwise(0)).alias("new_york_sqft"),
    )
    .orderBy("opened_month")
)
