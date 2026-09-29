from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("budget") < 3993000)
    .groupBy("location", "dept_name")
    .agg(
        (F.max("budget") - F.min("budget")).alias("budget_range"),
        F.countDistinct("budget").alias("unique_locations"),
    )
    .filter(F.col("unique_locations") > 5)
    .select("location", "dept_name", "budget_range", "unique_locations")
)
