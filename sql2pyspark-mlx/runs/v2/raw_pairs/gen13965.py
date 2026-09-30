from pyspark.sql import functions as F

result = (
    df_employees
    .filter(~F.col("is_remote") & F.col("dept_id").isin([5, 31]))
    .select("id", "salary", F.round("salary", 2).alias("salary_rounded"))
    .orderBy("salary")
)
