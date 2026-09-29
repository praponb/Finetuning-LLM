from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") == "Legal") & (F.col("location") != "Berlin"))
    .select(
        "id",
        "budget",
        F.initcap(F.trim("location")).alias("location_title"),
        F.concat_ws(",", "location", "dept_name").alias("location_dept_name_key"),
    )
)
