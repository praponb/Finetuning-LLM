from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "region",
        F.quarter("opened_date").alias("opened_quarter"),
        F.year("opened_date").alias("opened_year"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)
