from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") > 12)
    .select("level")
    .unionByName(df_employees.filter(F.col("salary") > 137000).select("level"))
    .orderBy(F.col("level").desc())
)
