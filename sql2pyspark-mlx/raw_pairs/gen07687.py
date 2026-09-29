from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("level")
    .pivot("job_title", ["Designer", "Engineer", "Director"])
    .agg(F.sum("salary"))
    .orderBy("level")
)
