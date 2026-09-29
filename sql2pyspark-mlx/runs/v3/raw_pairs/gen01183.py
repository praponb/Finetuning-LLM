from pyspark.sql import functions as F

result = (
    df_customers
    .select("signup_date")
    .unionByName(
        df_accounts
        .filter(F.col("opened_date").between("2023-10-23", "2024-10-01"))
        .select(F.col("opened_date").alias("signup_date"))
    )
    .distinct()
    .orderBy("signup_date")
)
