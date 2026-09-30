from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("job_title", "manager_id").orderBy("salary_whole", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("level")
w3 = Window.partitionBy("dept_id", "job_title").orderBy("salary", "id")
w4 = Window.partitionBy(F.year("hire_date"), "manager_id").orderBy("level_count_salary_whole", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("manager_id").orderBy("last3_min_salary_whole", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("manager_id").orderBy("latest_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("dept_id", "manager_id")
w8 = Window.partitionBy("dept_id").orderBy("hire_date", "id").rowsBetween(-3, -1)
salary_by_dept_id = df_employees.groupBy("dept_id").agg(F.min("salary").alias("min_employee_salary"))

base = (
    df_employees
    .filter(~F.col("level").eqNullSafe(F.lit("L3")))
    .select(
        "*",
        F.when(F.col("salary") >= 143000, "High").when(F.col("salary") >= 65000, "Medium").otherwise("Low").alias("salary_band"),
        F.ceil("salary").alias("salary_whole"),
    )
)

scored = (
    base
    .withColumn("last3_min_salary", F.min("salary").over(w))
    .withColumn("last3_avg_salary", F.avg("salary").over(w))
    .withColumn("last3_count_salary", F.count("salary").over(w))
    .withColumn("level_count_salary_whole", F.count("salary_whole").over(w2))
)

valid = (
    scored
    .filter((F.col("hire_date") >= F.to_date(F.lit("2023-06-26"))) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "salary_whole",
        "last3_min_salary",
        "last3_avg_salary",
        "level_count_salary_whole",
        F.floor("last3_min_salary").alias("last3_min_salary_whole"),
        F.concat("full_name", F.lit(" "), "email").alias("full_name_email"),
        F.coalesce("job_title", F.lit("N/A")).alias("job_title_clean"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_windows = (
    deduped
    .filter(F.col("job_title").isNotNull())
    .withColumn("level_count_salary_whole_avg_6_prior", F.avg("level_count_salary_whole").over(w4))
    .withColumn("last3_avg_salary_avg_6", F.avg("last3_avg_salary").over(w5))
    .withColumn("last_known_level_count_salary_whole", F.last("level_count_salary_whole", ignorenulls=True).over(w6))
)

metrics = (
    with_windows
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "hire_date",
        "salary_whole",
        "last3_min_salary",
        "last3_min_salary_whole",
        "full_name_email",
        "level_count_salary_whole_avg_6_prior",
        F.sum("salary").over(w7).alias("dept_id_sum_salary"),
        F.avg("level_count_salary_whole").over(w8).alias("level_count_salary_whole_avg_3_prior"),
    )
)

by_type = (
    metrics
    .groupBy("dept_id")
    .pivot("job_title", ["Engineer", "Director", "Analyst"])
    .agg(
        F.sum("last3_min_salary").alias("amount"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "dept_id",
        F.col("Engineer_amount").alias("engineer_amount"),
        F.col("Engineer_item_count").alias("engineer_item_count"),
        F.col("Director_amount").alias("director_amount"),
        F.col("Director_item_count").alias("director_item_count"),
        F.col("Analyst_amount").alias("analyst_amount"),
        F.col("Analyst_item_count").alias("analyst_item_count"),
    )
)

result = (
    by_type
    .join(salary_by_dept_id, on="dept_id", how="left")
    .select(
        "dept_id",
        "engineer_item_count",
        "director_amount",
        "director_item_count",
        "analyst_amount",
        "analyst_item_count",
        F.col("min_employee_salary"),
    )
    .orderBy(F.col("analyst_amount").desc())
)
