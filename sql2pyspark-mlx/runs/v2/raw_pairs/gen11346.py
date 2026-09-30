from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") >= 1678000) & F.col("budget").between(2297000, 2656000) & F.col("location").isNotNull())
    .select(
        "id",
        "location",
        "budget",
        F.concat("dept_name", F.lit("/"), "location").alias("dept_name_location"),
    )
    .orderBy("dept_name_location")
)
