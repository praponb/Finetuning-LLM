from pyspark.sql import functions as F

base = (
    df_employees
    .filter(F.col("full_name").like("%Cha%") | F.col("dept_id").isin([23, 30, 34]))
)

salary_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("salary") >= 160000, "high").when(F.col("salary") >= 75000, "mid").otherwise("low").alias("salary_bucket"),
        "manager_id",
    )
    .agg(
        F.max("salary").alias("max_salary"),
        F.sort_array(F.collect_set("level")).alias("level_list"),
    )
)

result = (
    salary_bucket_summary
    .filter(F.col("max_salary") < 121000)
    .select("salary_bucket", "manager_id", "max_salary", "level_list")
    .orderBy(F.col("manager_id").desc())
)
