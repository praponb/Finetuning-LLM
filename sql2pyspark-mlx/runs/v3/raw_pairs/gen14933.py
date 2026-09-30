from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("location").isin(["New York", "Chiang Mai", "Bangkok"]))
    .select("id", "budget")
    .orderBy(F.col("budget").desc())
)
