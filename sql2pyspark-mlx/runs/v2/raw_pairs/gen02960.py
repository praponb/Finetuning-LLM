from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([17, 23, 33]) & F.col("dept_id").isin([1, 16, 20, 26]))
    .withColumn("email_user", F.split(F.lower("email"), "@").getItem(0))
    .withColumn("is_remote_label", F.when(F.col("is_remote"), "Yes").otherwise("No"))
)
