from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.quarter("opened_date").alias("opened_quarter"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)
