from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.concat("dept_name", F.lit("/"), "location").alias("dept_name_location"),
    )
)
