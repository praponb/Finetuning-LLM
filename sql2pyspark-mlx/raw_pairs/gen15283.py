from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isNull())
    .select("id", "location")
    .orderBy(F.col("location").desc())
)
