from pyspark.sql import functions as F

result = (
    df_departments
    .filter((F.col("dept_name") != "Finance") & ~F.col("dept_name").isin(["HR", "Legal", "Engineering"]) & F.col("location").isin(["Bangkok", "Chiang Mai"]))
    .select("location")
    .orderBy(F.col("location").desc())
)
