from pyspark.sql import functions as F

result = (
    df_employees
    .select("dept_id", "manager_id")
    .subtract(
        df_employees
        .filter(F.lower(F.trim("email")) == "ivy@gmail.com")
        .select("dept_id", "manager_id")
    )
)
