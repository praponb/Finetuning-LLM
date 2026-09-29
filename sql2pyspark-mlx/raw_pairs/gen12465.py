from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("job_title").orderBy("hire_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("manager_id").orderBy(F.col("salary").desc())
w3 = Window.partitionBy(F.year("hire_date"), "job_title").orderBy("salary", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("dept_id").orderBy(F.col("job_title_length").desc_nulls_last(), "id")

with_windows = (
    df_employees
    .withColumn("first_known_job_title", F.first("job_title", ignorenulls=True).over(w_grp))
    .withColumn("salary_cume_dist", F.cume_dist().over(w2))
    .withColumn("first_known_level", F.first("level", ignorenulls=True).over(w3))
)

valid = (
    with_windows
    .filter(F.col("hire_date") >= F.to_date(F.lit("2023-07-21")))
    .select(
        "*",
        F.date_format("hire_date", "yyyy-MM").alias("hire_date_label"),
        F.date_format("hire_date", "MMM yyyy").alias("hire_date_label_2"),
        F.coalesce("level", F.lit("OTHER")).alias("level_clean"),
    )
)

filtered = (
    valid
    .filter((F.col("hire_date") > F.lit("2023-07-08").cast("date")) & (F.col("salary") > 135000))
    .withColumn("job_title_length", F.length("job_title"))
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .alias("a")
    .join(df_departments.alias("p"), F.col("a.dept_id") == F.col("p.id"), "left")
    .groupBy(
        F.col("p.dept_name").alias("dept_name"),
        F.col("p.location").alias("location"),
        F.col("a.level").alias("level"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.avg(F.col("a.salary_cume_dist")).alias("avg_salary_cume_dist"),
    )
)
