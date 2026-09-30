from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "location",
        "dept_name",
        "budget",
        F.concat("dept_name", F.lit(", "), "location").alias("dept_name_location"),
    )
)
