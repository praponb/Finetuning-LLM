from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.length("email") > 13)
    .groupBy("level")
    .pivot("job_title", ["Intern", "Engineer"])
    .agg(F.avg("salary"))
)
