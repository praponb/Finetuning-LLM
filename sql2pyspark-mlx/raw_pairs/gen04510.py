from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("level").orderBy("salary", "id")
w2 = Window.partitionBy("location", "job_title").orderBy(F.col("budget").desc_nulls_first(), "id")

ranked = (
    df_employees
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "level",
    )
)

enriched = (
    ranked
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.email",
        "s.dept_id",
        "s.manager_id",
        "s.job_title",
        "s.hire_date",
        "s.is_remote",
        F.col("d.location"),
        F.col("d.budget"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

with_dims = (
    deduped
    .alias("a")
    .join(df_employees.alias("e"), F.col("a.manager_id") == F.col("e.id"), "left")
    .filter(F.col("e.dept_id") == 26)
    .select(
        "a.id",
        "a.email",
        "a.dept_id",
        "a.manager_id",
        "a.job_title",
        "a.location",
        F.col("e.hire_date").alias("employee_hire_date"),
    )
)

result = (
    with_dims
    .groupBy(
        "manager_id",
        F.date_trunc("QUARTER", "employee_hire_date").alias("quarter_start"),
        "location",
    )
    .agg(
        F.count_if(F.col("job_title") == "Intern").alias("intern_events"),
        F.count(F.when(F.col("job_title") == "Analyst", 1)).alias("analyst_rows"),
        F.sort_array(F.collect_set("location")).alias("location_list"),
        F.count(F.when(F.col("location") == "New York", 1)).alias("new_york_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy("new_york_count")
)
