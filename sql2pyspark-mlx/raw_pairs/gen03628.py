from pyspark.sql import functions as F

cleaned = (
    df_employees
    .filter((F.col("hire_date") > F.to_date(F.lit("2023-06-30"))) & F.col("level").isNotNull())
    .select(
        "id",
        "full_name",
        "email",
        "dept_id",
        "job_title",
        "salary",
        "level",
        (F.datediff(F.current_date(), "hire_date") / 7).alias("weeks_since_hire_date"),
        F.coalesce("level", F.lit("N/A")).alias("resolved_level"),
    )
)

result = (
    cleaned
    .select("id", "level", "salary", "weeks_since_hire_date")
    .unpivot(
        ids=["id", "level"],
        values=["salary", "weeks_since_hire_date"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
