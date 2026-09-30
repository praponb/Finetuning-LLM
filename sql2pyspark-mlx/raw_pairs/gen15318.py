from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(121000, 146000))
    .select("email")
    .intersect(df_payments.select("status"))
    .orderBy("email")
)
