from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").isNotNull())
    .groupBy("job_title")
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.max("hire_date").alias("last_hire_date"),
    )
)
