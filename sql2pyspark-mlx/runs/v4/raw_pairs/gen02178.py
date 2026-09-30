from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy("salary", "id")

valid = (
    df_employees
    .filter((F.col("hire_date") > F.lit("2023-04-23").cast("date")) & F.col("job_title").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "is_remote",
        "level",
        F.dayofweek("hire_date").alias("hire_date_dayofweek"),
    )
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

with_dims = (
    deduped
    .alias("s")
    .join(df_departments.alias("d"), F.col("s.dept_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.dept_name"))
)

grouped = (
    with_dims
    .groupBy(
        F.when(F.col("salary") >= 142000, "LARGE").otherwise("SMALL").alias("salary_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.percentile_approx("hire_date_dayofweek", [0.25, 0.5, 0.75]).alias("hire_date_dayofweek_quartiles"),
        F.count(F.when(F.col("job_title") == "Manager", 1)).alias("manager_rows"),
    )
)

result = (
    grouped
    .select(
        "salary_tier",
        "job_title_list",
        "hire_date_dayofweek_quartiles",
        "manager_rows",
    )
    .orderBy(F.col("manager_rows").desc_nulls_first(), "salary_tier")
)
