from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("customer_id") == 21)
    .select("opened_date")
    .union(
        df_employees
        .filter(F.col("hire_date") < "2024-06-15")
        .select("hire_date")
    )
    .distinct()
    .orderBy("opened_date")
)
