from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("name") > 9)
    .select("id", "email")
    .unionByName(
        df_customers
        .filter(F.col("signup_date") >= "2023-10-04")
        .select("id", "email")
    )
    .distinct()
)
