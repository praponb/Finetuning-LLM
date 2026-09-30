from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("level").isin(["L1", "L2"]))
    .groupBy(F.year("hire_date").alias("hire_year"))
    .pivot("job_title", ["Director", "Intern", "Analyst", "Designer"])
    .agg(F.count("salary"))
    .orderBy("hire_year")
)
