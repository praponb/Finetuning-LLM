from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        "dept_name",
        F.initcap(F.trim("location")).alias("location_title"),
    )
    .orderBy("location_title")
)
