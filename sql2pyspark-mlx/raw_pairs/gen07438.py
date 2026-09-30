from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("salary").between(81000, 98000))
    .select("email", "manager_id")
    .intersect(
        df_employees
        .filter(F.col("full_name").isNotNull())
        .select("email", "manager_id")
    )
    .orderBy("email")
)
