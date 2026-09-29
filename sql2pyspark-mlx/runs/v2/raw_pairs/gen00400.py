from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.length("email") >= 14)
    .select("id", "country")
    .unionByName(
        df_customers
        .filter(F.col("signup_date").between("2024-09-11", "2025-03-01"))
        .select("id", "country")
    )
    .distinct()
    .orderBy("id")
)
