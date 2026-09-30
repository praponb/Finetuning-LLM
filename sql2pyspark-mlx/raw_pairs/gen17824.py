from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age").between(47, 54))
    .select("city")
    .unionByName(
        df_customers
        .filter(F.col("signup_date") <= "2024-05-01")
        .select("city")
    )
    .distinct()
)
