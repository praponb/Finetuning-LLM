from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("city", F.month("opened_date").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.count(F.when(F.col("region") == "Central", 1)).alias("central_count"),
    )
    .filter(F.col("central_count") > 8)
    .orderBy(F.col("opened_month").desc(), "city")
)
