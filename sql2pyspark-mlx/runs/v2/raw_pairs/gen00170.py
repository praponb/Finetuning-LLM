from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("is_remote"))
    .select("id", "dept_id", "email")
    .unionByName(
        df_employees
        .filter(F.col("manager_id").isNotNull())
        .select("id", "dept_id", "email")
    )
)
