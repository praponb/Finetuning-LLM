from pyspark.sql import functions as F

result = (
    df_employees
    .withColumn("salary_band", F.when(F.col("salary") >= 143000, "High").when(F.col("salary") >= 70000, "Medium").otherwise("Low"))
    .withColumn("salary_adjusted", F.round((F.col("salary") * 100), 2))
)
