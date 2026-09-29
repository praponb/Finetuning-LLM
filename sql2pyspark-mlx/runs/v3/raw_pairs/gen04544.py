from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("manager_id").isNull())
    .select("hire_date")
    .unionByName(
        df_employees
        .filter(F.col("email").like("%@gmail.com"))
        .select("hire_date")
    )
    .distinct()
)
