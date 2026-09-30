from pyspark.sql import functions as F

src = (
    df_employees
    .filter(F.col("full_name").like("Ivy%") & ((F.col("level") == "L1") | F.col("salary").between(101000, 129000)))
)

summary = (
    src
    .groupBy("is_remote")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    summary
    .filter(F.col("max_salary") > 66000)
    .select("is_remote", "max_salary")
    .orderBy(F.col("max_salary").desc())
)
