from pyspark.sql import functions as F

result = (
    df_departments
    .filter(F.col("dept_name").isin(["Legal", "HR"]))
    .groupBy("location")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("location") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
    .filter(F.col("pct_bangkok") < 27)
    .select("location", "pct_bangkok")
    .orderBy(F.col("pct_bangkok").desc())
)
