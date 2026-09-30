from pyspark.sql import functions as F

result = (
    df_customers
    .select("email")
    .subtract(
        df_employees
        .filter(F.month("hire_date").isin([6, 10, 11]))
        .select("email")
    )
)
