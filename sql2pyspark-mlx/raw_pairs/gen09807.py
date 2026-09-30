from pyspark.sql import functions as F

base = (
    df_departments
    .filter(~F.col("dept_name").isin(["Engineering", "Sales", "Finance"]) & (F.col("dept_name") == "Legal"))
    .withColumn("clean_dept_name", F.trim("dept_name"))
)

summary = (
    base
    .groupBy("dept_name", "location")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("location"))).alias("location_values"),
    )
)

result = (
    summary
    .orderBy("location_values", "dept_name", "location")
    .limit(50)
)
