from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Berlin")
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.initcap(F.trim("location")).alias("location_title"),
    )
    .orderBy("location", "budget")
)
