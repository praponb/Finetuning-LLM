from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("dept_id").orderBy(F.col("salary_int_adjusted").desc_nulls_first(), "id")

filtered = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-04-02").cast("date")) & F.col("manager_id").isNotNull())
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        F.add_months("hire_date", 3).alias("hire_date_plus_months"),
    )
)

base = (
    filtered
    .filter(F.col("dept_id").isin([27, 31]) & (~F.col("job_title").eqNullSafe(F.lit("Intern"))))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "hire_date_plus_months",
        F.col("salary").cast("int").alias("salary_int"),
        F.date_format("hire_date_plus_months", "yyyy-MM-dd").alias("hire_date_plus_months_label"),
        F.coalesce("job_title", F.lit("DIRECT")).alias("job_title_filled"),
    )
)

cleaned = (
    base
    .filter((F.col("job_title").eqNullSafe(F.lit("Intern"))) & (F.year("hire_date_plus_months") >= 2025))
    .select(
        "id",
        "dept_id",
        "manager_id",
        "job_title",
        "salary",
        "hire_date",
        "is_remote",
        "hire_date_plus_months",
        "salary_int",
        "hire_date_plus_months_label",
        "job_title_filled",
        F.round("salary_int", 1).alias("salary_int_rounded"),
        F.round((F.col("salary_int") * 0.85), 2).alias("salary_int_adjusted"),
    )
)

deduped = (
    cleaned
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .groupBy(F.date_trunc("YEAR", "hire_date_plus_months").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("job_title")).alias("job_title_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("job_title")).alias("job_title_list_2"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("year_start").asc_nulls_last())
)
