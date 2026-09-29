from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isNull())
    .select("dept_name", "location")
    .distinct()
    .orderBy(F.col("dept_name").desc(), "location")
    .limit(3)
)
