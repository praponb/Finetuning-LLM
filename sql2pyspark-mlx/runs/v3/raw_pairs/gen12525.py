from pyspark.sql import functions as F

result = (
    df_employees
    .filter(F.col("hire_date").between("2023-06-01", "2023-10-21"))
    .select("full_name")
    .intersect(
        df_employees
        .filter(F.col("email").like("%@startup.io"))
        .select("full_name")
    )
)
