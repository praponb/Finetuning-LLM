from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([1, 2, 23, 34]) & ((F.col("salary") <= 127000) | F.col("full_name").isNull()))
    .withColumn("salary_tier", F.when(F.col("salary") >= 149000, "High").when(F.col("salary") >= 86000, "Medium").otherwise("Low"))
)
