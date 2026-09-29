from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNull() & (F.col("dept_name") == "Marketing"))
    .select("id", "location", F.initcap(F.trim("location")).alias("location_title"))
)
