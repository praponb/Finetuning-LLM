from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isin([2, 27]))
    .select("full_name", "hire_date")
    .unionByName(
        df_employees
        .filter(F.col("is_remote"))
        .select("full_name", "hire_date")
    )
    .distinct()
)
