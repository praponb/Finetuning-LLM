from pyspark.sql import functions as F

result = (
    df_customers
    .select("email")
    .unionByName(
        df_employees
        .filter(F.col("email").like("%@gmail.com"))
        .select("email")
    )
    .distinct()
    .orderBy(F.col("email").desc())
)
