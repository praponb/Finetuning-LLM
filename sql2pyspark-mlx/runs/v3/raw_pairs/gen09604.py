from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .pivot("job_title", ["Analyst", "Engineer", "Director", "Designer"])
    .agg(F.sum("salary"))
    .orderBy("level")
)
