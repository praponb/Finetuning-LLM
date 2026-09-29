from pyspark.sql import functions as F

cleaned = (
    df_departments
    .filter(F.col("location").isNotNull() & (F.col("budget") >= 3896000))
    .withColumn("clean_location", F.trim("location"))
    .withColumn("resolved_dept_name", F.coalesce("dept_name", F.lit("NONE")))
)

result = (
    cleaned
    .select("id", "location")
    .intersect(
        df_departments
        .filter(F.col("dept_name").isin(["Sales", "Legal", "Finance"]))
        .select("id", "location")
    )
)
