from pyspark.sql import functions as F

result = (
    df_departments
    .select(
        "id",
        "location",
        "dept_name",
        "budget",
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"),
    )
    .orderBy(F.col("id").desc(), "location")
)
