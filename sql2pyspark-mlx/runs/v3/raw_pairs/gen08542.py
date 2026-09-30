from pyspark.sql import functions as F

base = df_employees.filter(F.col("salary") >= 138000)

agg = (
    base
    .groupBy("level")
    .agg(F.sort_array(F.collect_set("job_title")).alias("job_title_list"))
)

result = agg.orderBy(F.col("level").desc())
