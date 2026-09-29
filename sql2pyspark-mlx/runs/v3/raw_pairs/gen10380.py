from pyspark.sql import functions as F

result = (
    df_products
    .groupBy("supplier_id")
    .agg(
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.max("launch_date").alias("last_launch_date"),
    )
)
