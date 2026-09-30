from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.upper("store_name").like("%IRP%"))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)
