from pyspark.sql import functions as F

result = (
    df_departments
    .groupBy("dept_name")
    .agg(
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("location") == "Bangkok", 1)).alias("bangkok_count"),
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)
