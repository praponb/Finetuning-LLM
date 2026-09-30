from pyspark.sql import functions as F

result = (
    df_departments
    .filter(~F.col("dept_name").isin(["Sales", "Finance", "Engineering"]))
    .select("dept_name", F.lit("department").alias("kind"))
    .union(df_stores.select("region", F.lit("store").alias("kind")))
    .distinct()
    .orderBy(F.col("dept_name").desc())
)
