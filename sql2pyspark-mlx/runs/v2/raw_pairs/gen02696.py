from pyspark.sql import functions as F

result = (
    df_employees
    .select(
        "dept_id",
        "hire_date",
        "level",
        F.date_sub("hire_date", 3).alias("hire_date_minus_3d"),
        F.coalesce("full_name", F.lit("unassigned")).alias("full_name_filled"),
    )
    .orderBy("dept_id")
)
