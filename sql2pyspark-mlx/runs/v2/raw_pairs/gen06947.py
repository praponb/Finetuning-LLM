from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("dept_id").isin([19, 38]) & F.col("dept_id").isin([12, 20]))
    .select("id", "dept_id")
    .orderBy("dept_id", "id")
    .limit(10)
)
