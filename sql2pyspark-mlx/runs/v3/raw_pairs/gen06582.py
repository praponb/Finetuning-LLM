from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") > 64000)
    .groupBy("is_remote", F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Director", 1).otherwise(0))) / F.count("*")), 1).alias("pct_director"),
    )
    .filter(F.col("pct_director") > 62)
    .select("is_remote", "hire_quarter", "pct_director")
    .orderBy(F.col("hire_quarter").desc(), "is_remote")
    .limit(20)
)
