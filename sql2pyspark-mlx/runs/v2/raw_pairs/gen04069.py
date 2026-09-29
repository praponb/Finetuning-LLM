from pyspark.sql import functions as F

base = df_employees.filter((F.col("salary") < 117000) | (F.col("salary") > 107000))

agg = (
    base
    .groupBy("dept_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("job_title"))).alias("job_title_values"),
        F.sum(F.when(F.col("level") == "L3", F.col("salary")).otherwise(0)).alias("l3_salary"),
    )
)

result = agg.orderBy("job_title_values")
