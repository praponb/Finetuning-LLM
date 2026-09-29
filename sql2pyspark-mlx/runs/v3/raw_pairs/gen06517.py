from pyspark.sql import functions as F

filtered_employees = df_employees.filter(F.col("level") == "L3")

summary = (
    filtered_employees
    .groupBy("level", "is_remote")
    .agg(
        F.sort_array(F.collect_set("level")).alias("level_list"),
        (F.max("salary") - F.min("salary")).alias("salary_range"),
    )
)

result = summary.select("level", "is_remote", "salary_range")
