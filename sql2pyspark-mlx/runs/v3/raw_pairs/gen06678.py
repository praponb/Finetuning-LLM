from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.upper("email").like("%RAC%"))
    .select("hire_date")
    .subtract(df_accounts.select("opened_date"))
    .orderBy(F.col("hire_date").desc())
)
