from pyspark.sql import functions as F

result = (
    df_employees
    .filter((F.col("salary") > 143000) & F.col("level").isin(["L2", "L5", "L3"]) & F.col("dept_id").isin([8, 22]))
    .withColumn("salary_rounded", F.round("salary", 1))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)
