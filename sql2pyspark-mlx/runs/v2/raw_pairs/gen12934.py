from pyspark.sql import functions as F

result = (
    df_customers
    .groupBy("country")
    .agg(F.sort_array(F.collect_set("segment")).alias("segment_list"))
    .orderBy(F.col("country").desc())
)
