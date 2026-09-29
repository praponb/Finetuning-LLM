from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([4, 10, 20, 35]))
    .groupBy(F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
    )
    .filter(F.col("l4_salary") >= 64)
    .orderBy(F.col("l4_salary").desc())
)
