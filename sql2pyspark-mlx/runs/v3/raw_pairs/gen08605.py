from pyspark.sql import functions as F

src = df_stores.filter(F.col("region") != "South")

stores_stats = (
    src
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)

result = stores_stats.orderBy("region_values")
