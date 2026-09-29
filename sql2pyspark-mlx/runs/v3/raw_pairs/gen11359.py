from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Berlin", "Singapore"]))
    .select(
        "location",
        F.substring("dept_name", 1, 4).alias("dept_name_prefix"),
        F.initcap(F.trim("location")).alias("location_title"),
    )
    .orderBy("location_title")
)
