from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("hire_date") > "2024-12-01") & (F.col("dept_id") == 29))
    .agg(
        F.countDistinct("salary").alias("unique_depts"),
        F.sum(F.when(F.col("level") == "L4", F.col("salary")).otherwise(0)).alias("l4_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)
