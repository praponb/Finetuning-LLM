from pyspark.sql import functions as F

result = (
    df_employees
    .select("hire_date")
    .union(df_subscriptions.select("start_date"))
    .orderBy(F.col("hire_date").desc())
)
