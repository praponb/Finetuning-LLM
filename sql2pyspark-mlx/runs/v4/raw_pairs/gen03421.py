from pyspark.sql import functions as F

base = (
    df_employees
    .filter((F.col("level") != "L5") & (F.col("manager_id") == 21))
    .withColumn("hire_date_label", F.date_format("hire_date", "yyyy-MM"))
)

summary = (
    base
    .groupBy(F.date_format("hire_date", "yyyy-MM").alias("hire_month"))
    .agg(
        F.sum(F.when(F.col("level") == "L2", F.col("salary")).otherwise(0)).alias("l2_salary"),
    )
)

result = (
    summary
    .filter(F.col("l2_salary") < 80)
    .select("hire_month", "l2_salary")
)
