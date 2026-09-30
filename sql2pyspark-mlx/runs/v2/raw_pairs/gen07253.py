from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("full_name") > 8)
    .groupBy("level")
    .pivot("job_title", ["Intern", "Designer"])
    .agg(F.max("salary"))
)
