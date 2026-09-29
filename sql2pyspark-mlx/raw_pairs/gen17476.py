from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Singapore"]))
    .select(
        "id",
        "budget",
        "location",
        F.concat("dept_name", F.lit(", "), "location").alias("dept_name_location"),
    )
)
