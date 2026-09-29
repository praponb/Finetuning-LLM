from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("city_length").desc(), "id")

valid = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-07-01").cast("date"))
    .withColumn("opened_date_minus_81d", F.date_sub("opened_date", 81))
)

filtered = (
    valid
    .filter(F.col("region").isNotNull())
    .withColumn("city_length", F.length("city"))
)

deduped = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_name", "region", "opened_date_minus_81d", "city_length")
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.store_id").isin([2, 4])), "left_anti")
)

result = (
    not_flagged
    .rollup("region")
    .agg(
        F.round(F.sum("city_length") / F.nullif(F.count(F.when(F.col("region") == "West", 1)), F.lit(0)), 2).alias("city_length_per_west"),
        F.avg("city_length").alias("avg_city_length"),
        F.size(F.collect_set("region")).alias("num_regions"),
        F.grouping_id().alias("grouping_id"),
    )
    .filter(F.col("avg_city_length") >= 0)
)
