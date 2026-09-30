from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("location") == "Chiang Mai") & (F.col("dept_name") == "Marketing") & F.col("budget").between(1084000, 2088000))
    .select(
        "id",
        "location",
        "dept_name",
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
        F.lower("dept_name").alias("dept_name_lower"),
    )
)
