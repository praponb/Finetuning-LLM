from pyspark.sql import functions as F

result = (
    df_employees.alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .filter(F.col("d.dept_name") == "Engineering")
    .groupBy(F.col("d.location"))
    .agg(
        F.count("*").alias("num_employees"),
        F.max(F.col("e.salary")).alias("max_salary"),
    )
    .filter(F.col("max_salary") > 143000)
    .orderBy("location")
)
