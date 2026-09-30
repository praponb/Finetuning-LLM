from pyspark.sql import functions as F

filtered = (
    df_departments
    .filter(F.col("dept_name").isNotNull() & (~F.col("location").eqNullSafe(F.lit("New York"))))
    .withColumn("dept_name_upper", F.upper("dept_name"))
    .withColumn("resolved_location", F.coalesce("location", F.lit("N/A")))
)

result = (
    filtered
    .select("id", "dept_name")
    .intersectAll(
        df_departments
        .filter(F.col("location") != "New York")
        .select("id", "dept_name")
    )
)
