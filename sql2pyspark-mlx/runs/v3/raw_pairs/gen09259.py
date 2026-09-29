from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("id", "full_name", "hire_date")
    .unionByName(
        df_employees
        .filter(F.col("manager_id").isNotNull())
        .select("id", "full_name", "hire_date")
    )
    .orderBy("id")
)
