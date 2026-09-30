from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Tokyo") & ((F.col("budget") > 3762000) | F.col("location").isin(["Tokyo", "Singapore"])))
    .select(
        "id",
        "budget",
        "dept_name",
        F.concat("dept_name", F.lit("/"), "location").alias("dept_name_location"),
    )
)
