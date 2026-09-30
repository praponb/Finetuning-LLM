from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id") == 21)
    .select("full_name")
    .unionByName(
        df_employees
        .filter(F.col("email").like("%@outlook.com"))
        .select("full_name")
    )
)
