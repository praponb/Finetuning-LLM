from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Marketing", "HR"]) & (F.col("budget") > 2413000))
    .select(
        "id",
        "location",
        "dept_name",
        F.col("budget").cast("int").alias("budget_int"),
        F.concat("location", F.lit(" "), "dept_name").alias("location_dept_name"),
    )
    .orderBy(F.col("id").desc())
)
