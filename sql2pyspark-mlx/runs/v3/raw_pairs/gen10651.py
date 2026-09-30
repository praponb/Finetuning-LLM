from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("age") == 40)
    .select("signup_date")
    .unionByName(
        df_customers
        .filter(F.col("signup_date") > "2023-03-01")
        .select("signup_date")
    )
)
