from pyspark.sql import functions as F

filtered_employees = (
    df_employees
    .filter((F.col("salary") < 143000) & (F.col("salary") < 74000))
    .withColumn("salary_tier", F.when(F.col("salary") >= 138000, "High").when(F.col("salary") >= 73000, "Medium").otherwise("Low"))
)

hire_month_summary = (
    filtered_employees
    .groupBy(F.month("hire_date").alias("hire_month"), "manager_id")
    .agg(
        F.countDistinct("salary").alias("unique_levels"),
        F.sum(F.when(F.col("level") == "L5", F.col("salary")).otherwise(0)).alias("l5_salary"),
    )
)

result = hire_month_summary.select("hire_month", "manager_id", "unique_levels", "l5_salary")
