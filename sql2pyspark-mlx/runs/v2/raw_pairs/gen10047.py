from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNotNull())
    .select(
        "id",
        "dept_name",
        "location",
        "budget",
        F.coalesce("location", F.lit("Unknown")).alias("location_filled"),
        F.upper("location").alias("location_upper"),
    )
    .orderBy(F.col("budget").desc())
)
