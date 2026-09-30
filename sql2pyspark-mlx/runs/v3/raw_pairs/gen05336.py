from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy(F.date_trunc("MONTH", "hire_date").alias("hire_month"), "is_remote")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("level"))).alias("level_values"),
        F.min("salary").alias("min_salary"),
    )
    .filter(F.col("min_salary") >= 63000)
    .orderBy("hire_month")
)
