from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").isNotNull())
    .select("hire_date")
    .union(
        df_accounts
        .filter(F.col("opened_date").isNotNull())
        .select("opened_date")
    )
    .distinct()
)
