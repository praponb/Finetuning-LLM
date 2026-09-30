from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~(F.col("dept_name").isin(["Legal", "Marketing"]) & F.col("budget").isNull()))
    .select(
        "id",
        "location",
        "budget",
        F.lower("dept_name").alias("dept_name_lower"),
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"),
    )
    .orderBy(F.col("id").desc())
)
