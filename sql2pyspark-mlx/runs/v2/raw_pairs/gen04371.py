from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location") == "Chiang Mai")
    .select(
        "id",
        "dept_name",
        F.substring("dept_name", 1, 4).alias("dept_name_prefix"),
        F.coalesce("location", F.lit("N/A")).alias("location_filled"),
    )
)
