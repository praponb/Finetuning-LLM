from pyspark.sql import functions as F

filtered_departments = df_departments.filter((F.col("budget") <= 2030000) | (F.col("budget") < 1371000))

dept_name_summary = (
    filtered_departments
    .groupBy("dept_name")
    .agg(
        F.max("budget").alias("max_budget"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
    )
)

result = (
    dept_name_summary
    .filter(F.col("max_budget") < 2558000)
    .orderBy(F.col("max_budget").desc())
)
