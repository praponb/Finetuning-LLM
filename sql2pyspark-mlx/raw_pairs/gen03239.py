from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("budget") < 1349000) & (F.col("budget") > 1981000))
    .select(
        "id",
        "budget",
        F.coalesce("location", F.lit("unassigned")).alias("location_filled"),
        F.upper("location").alias("location_upper"),
    )
)
