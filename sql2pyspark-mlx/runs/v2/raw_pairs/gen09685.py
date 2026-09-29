from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .pivot("job_title", ["Engineer", "Intern", "Designer"])
    .agg(F.sum("salary"))
)
