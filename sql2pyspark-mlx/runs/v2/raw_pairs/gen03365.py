from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([13, 14, 23]) & ((F.upper(F.trim("email")) == "KANYA@MAIL.CO.TH") | (F.length("email") >= 14)))
    .groupBy(
        F.when(F.col("salary") >= 151000, "high").when(F.col("salary") >= 55000, "mid").otherwise("low").alias("salary_bucket"),
    )
    .agg(
        F.sum(F.when(F.col("job_title") == "Designer", F.col("salary")).otherwise(0)).alias("designer_salary"),
        F.count(F.when(F.col("job_title") == "Intern", 1)).alias("intern_count"),
        F.max("hire_date").alias("last_hire_date"),
    )
)
