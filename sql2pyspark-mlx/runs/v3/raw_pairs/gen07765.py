from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary") <= 107000)
    .select("id", "is_remote")
    .orderBy("id")
    .limit(5)
)
