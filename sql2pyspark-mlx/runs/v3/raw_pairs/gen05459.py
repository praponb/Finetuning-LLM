from pyspark.sql import functions as F

result = (
    df_products
    .groupBy(F.date_trunc("MONTH", "launch_date").alias("launch_month"))
    .agg(F.sort_array(F.collect_set("brand")).alias("brand_list"))
)
