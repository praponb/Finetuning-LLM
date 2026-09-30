from pyspark.sql import functions as F

result = (
    df_store_sales
    .cube("store_id")
    .agg(
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids_2"),
        F.stddev_samp("units").alias("units_stddev"),
        F.corr("revenue", "units").alias("corr_revenue_units"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids_3"),
    )
)
