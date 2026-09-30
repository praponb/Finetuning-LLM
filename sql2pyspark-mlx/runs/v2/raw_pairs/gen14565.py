from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("location").isin(["London", "Bangkok", "New York"]))
    .select(
        "id",
        "location",
        "budget",
        F.coalesce("dept_name", F.lit("N/A")).alias("dept_name_filled"),
        F.concat("dept_name", F.lit(", "), "location").alias("dept_name_location"),
    )
)
