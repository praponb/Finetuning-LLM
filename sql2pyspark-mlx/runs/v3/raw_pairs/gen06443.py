from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("email").like("%@company.co") & (F.col("dept_id") == 40))
    .withColumn("salary_band", F.when(F.col("salary") >= 147000, "High").when(F.col("salary") >= 41000, "Medium").otherwise("Low"))
    .withColumn("full_name_lower", F.lower("full_name"))
)
