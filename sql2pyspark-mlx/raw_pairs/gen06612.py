from pyspark.sql import functions as F

result = (
    df_employees
    .alias("src")
    .join(df_departments.alias("d"), F.col("src.dept_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["location", "dept_name"], ["location"]],
        F.col("d.location").alias("location"),
        F.col("d.dept_name").alias("dept_name"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("src.salary")).alias("total_salary"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("dept_name").asc_nulls_first())
)
