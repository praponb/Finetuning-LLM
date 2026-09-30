from pyspark.sql import functions as F

src = df_departments.filter(F.col("dept_name").isin(["Sales", "Marketing"]))

summary = (
    src
    .groupBy("location")
    .agg(F.count("*").alias("row_count"))
)

result = (
    summary
    .select("location", "row_count")
    .orderBy("row_count")
)
