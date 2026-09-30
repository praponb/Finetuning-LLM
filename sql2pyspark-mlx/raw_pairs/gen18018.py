from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(114000, 132000) & F.col("level").isin(["L3", "L4"]))
    .withColumn("full_name_level", F.concat("full_name", F.lit(", "), "level"))
    .withColumn("salary_level", F.when(F.col("salary") >= 160000, "High").when(F.col("salary") >= 42000, "Medium").otherwise("Low"))
)
