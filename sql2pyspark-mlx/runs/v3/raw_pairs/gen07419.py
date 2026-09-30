from pyspark.sql import functions as F

valid_employees = (
    df_employees
    .filter(~F.col("level").isin(["L5", "L1"]))
    .withColumn("email_full_name_key", F.concat_ws("-", "email", "full_name"))
)

agg = (
    valid_employees
    .groupBy("level")
    .agg(F.max("salary").alias("max_salary"))
)

result = (
    agg
    .filter(F.col("max_salary") >= 61000)
    .select("level", "max_salary")
    .orderBy(F.col("max_salary").desc())
)
