from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        "region",
        "city",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
    .orderBy("city")
)
