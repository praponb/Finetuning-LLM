from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L1", "L3"]))
    .groupBy("level", F.quarter("hire_date").alias("hire_quarter"))
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
        F.round(((100 * F.sum(F.when(F.col("job_title") == "Manager", 1).otherwise(0))) / F.count("*")), 1).alias("pct_manager"),
    )
)
