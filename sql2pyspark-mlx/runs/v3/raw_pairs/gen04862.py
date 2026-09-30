from pyspark.sql import functions as F

src = df_stores.filter(~F.col("store_name").like("M%"))

stores_stats = (
    src
    .groupBy("region")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(((100 * F.sum(F.when(F.col("city") == "New York", 1).otherwise(0))) / F.count("*")), 1).alias("pct_new_york"),
    )
)

result = (
    stores_stats
    .filter(F.col("pct_new_york") > 73)
    .orderBy(F.col("pct_new_york").desc())
)
