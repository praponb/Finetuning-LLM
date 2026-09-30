from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy("region")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
        F.max("opened_date").alias("last_opened_date"),
    )
    .orderBy(F.col("last_opened_date").desc())
)
