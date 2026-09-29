from pyspark.sql import functions as F

src = (
    df_stores
    .filter(F.col("region") == "East")
    .withColumn("sqft_rounded", F.round("sqft", 0))
)

summary = (
    src
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(F.sort_array(F.collect_set("region")).alias("region_list"))
)

result = (
    summary
    .select("opened_quarter", "region_list")
    .orderBy(F.col("opened_quarter").desc())
    .limit(1)
)
