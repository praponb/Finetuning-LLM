from pyspark.sql import functions as F

result = (
    df_employees
    .groupBy("is_remote")
    .pivot("job_title", ["Manager", "Director"])
    .agg(F.count("salary"))
    .orderBy("is_remote")
)
