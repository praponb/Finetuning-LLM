from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["Tokyo", "Singapore"]) & F.col("location").isNotNull())
    .select(
        "budget",
        "dept_name",
        "location",
        F.coalesce("location", F.lit("Unknown")).alias("location_filled"),
    )
)
