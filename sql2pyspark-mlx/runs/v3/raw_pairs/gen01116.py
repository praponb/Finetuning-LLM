from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .pivot("job_title", ["Intern", "Designer", "Director", "Engineer"])
    .agg(F.max("salary"))
    .orderBy("level")
)
