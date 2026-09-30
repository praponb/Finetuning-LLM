from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "Marketing"]))
    .select(
        "budget",
        "location",
        "dept_name",
        F.concat("location", F.lit(" - "), "dept_name").alias("location_dept_name"),
    )
)
