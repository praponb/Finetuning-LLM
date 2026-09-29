from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote") & (F.col("full_name").like("Kan%") | (F.col("level") != "L1")))
    .groupBy(F.month("hire_date").alias("hire_month"))
    .agg(
        F.count(F.when(F.col("level") == "L3", 1)).alias("l3_count"),
        F.sum("salary").alias("total_salary"),
    )
)
