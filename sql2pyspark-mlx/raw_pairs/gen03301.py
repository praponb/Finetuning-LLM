from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(~F.col("location").isin(["Berlin", "London"]))
    .select(
        "*",
        F.concat_ws("/", "location", "dept_name").alias("location_dept_name_key"),
        F.coalesce("location", F.lit("OTHER")).alias("location_clean"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_employees.alias("ch"), (F.col("ch.dept_id") == F.col("s.id")) & (F.col("ch.is_remote")), "left_anti")
)
