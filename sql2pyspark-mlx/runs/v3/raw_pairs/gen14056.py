from pyspark.sql import functions as F

result = (
    df_customers
    .select("email")
    .unionByName(
        df_employees
        .filter(F.col("email").like("%@startup.io"))
        .select("email")
    )
    .orderBy("email")
)
