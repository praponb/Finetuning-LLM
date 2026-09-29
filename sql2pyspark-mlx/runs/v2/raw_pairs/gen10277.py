from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("region") != "North") & (F.col("city") == "New York"))
    .groupBy("city")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Tokyo", 1).otherwise(0))) / F.count("*")), 1).alias("pct_tokyo"),
    )
    .orderBy("pct_tokyo", "city")
    .limit(10)
)
