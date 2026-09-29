from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("email").like("%@yahoo.com") & F.col("salary").between(62000, 100000))
)

summary = (
    src
    .groupBy("level")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    summary
    .filter(F.col("max_salary") > 99000)
    .orderBy(F.col("max_salary").desc(), "level")
    .limit(50)
)
