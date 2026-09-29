from pyspark.sql import functions as F

result = (
    df_departments
    .agg(
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.count("*").alias("total_count"),
    )
    .orderBy(F.col("total_count").desc())
)
