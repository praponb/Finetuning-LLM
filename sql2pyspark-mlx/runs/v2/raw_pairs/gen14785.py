from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("level").isin(["L2", "L4", "L1"]) & (F.col("salary") < 73000))
    .groupBy("manager_id")
    .agg(F.max("hire_date").alias("latest_hire_date"))
    .orderBy(F.col("manager_id").desc())
)
