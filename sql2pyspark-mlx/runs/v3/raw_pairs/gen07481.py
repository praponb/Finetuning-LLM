from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("country") != "SG")
    .select("id", "country")
    .unionByName(
        df_customers
        .filter(F.col("signup_date") > "2024-04-01")
        .select("id", "country")
    )
    .distinct()
)
