from pyspark.sql import functions as F

result = (
    df_stores
    .filter((F.col("sqft") < 12852) & F.col("city").isin(["New York", "Bangkok", "Tokyo"]))
    .groupBy("city", F.month("opened_date").alias("opened_month"), "region")
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.round(((100 * F.sum(F.when(F.col("region") == "South", 1).otherwise(0))) / F.count("*")), 1).alias("pct_south"),
        F.count("sqft").alias("sqft_count"),
    )
)
