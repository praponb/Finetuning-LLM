from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("hire_date") >= F.lit("2023-05-01").cast("date")) & F.col("manager_id").isNotNull())
    .select("*", F.last_day("hire_date").alias("hire_date_month_end"))
)

enriched = (
    base
    .alias("e")
    .join(df_departments.alias("d"), F.col("e.dept_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.location"), F.col("d.dept_name"))
)

cleaned = (
    enriched
    .filter(F.col("dept_id") == 33)
    .select(
        "id",
        "email",
        "dept_id",
        "salary",
        "hire_date",
        "hire_date_month_end",
        "location",
        "dept_name",
        F.concat_ws(",", "email", "location").alias("email_location_key"),
    )
)

result = (
    cleaned
    .cube(
        F.date_trunc("YEAR", "hire_date").alias("year_start"),
        F.date_trunc("MONTH", "hire_date_month_end").alias("activity_month"),
    )
    .agg(
        F.round(F.avg("salary"), 2).alias("avg_salary"),
        F.round(F.sum("salary") / F.nullif(F.count(F.when(F.col("dept_name") == "Finance", 1)), F.lit(0)), 2).alias("salary_per_finance"),
        F.sum(F.when(F.col("location") == "Chiang Mai", F.col("salary"))).alias("chiang_mai_total_salary"),
    )
    .orderBy(F.col("activity_month").desc_nulls_last())
)
