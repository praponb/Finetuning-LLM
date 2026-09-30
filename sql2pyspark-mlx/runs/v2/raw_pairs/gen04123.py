from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") == "SG")
    .select("email", "city")
    .unionByName(
        df_customers
        .filter(F.col("credit_limit") <= 11100)
        .select("email", "city")
    )
    .distinct()
    .orderBy("city")
)
