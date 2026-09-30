from pyspark.sql import functions as F

filtered_departments = (
    df_departments
    .filter(((F.col("budget") > 2154000) & (F.col("budget") < 2256000)) | (F.col("budget") > 2698000))
    .withColumn("location_dept_name", F.concat("location", F.lit(" - "), "dept_name"))
)

dept_name_summary = (
    filtered_departments
    .groupBy("dept_name")
    .agg(F.max("budget").alias("max_budget"))
)

result = (
    dept_name_summary
    .filter(F.col("max_budget") < 3400000)
    .orderBy(F.col("max_budget").desc())
)
