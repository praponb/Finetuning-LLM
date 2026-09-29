from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "New York")
    .select("email")
    .unionByName(
        df_employees
        .filter(F.col("email").like("%@mail.co.th"))
        .select("email")
    )
    .distinct()
)
