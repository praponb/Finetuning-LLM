from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .groupBy("dept_id")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        F.sum("salary").alias("total_salary"),
    )
    .filter(F.col("total_salary") < 432000)
    .orderBy(F.col("dept_id").desc())
)
