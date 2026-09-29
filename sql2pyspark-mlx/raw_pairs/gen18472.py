from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("signup_date") > "2023-05-19")
    .select("city", "country")
    .unionByName(
        df_customers
        .filter(F.col("city") != "Bangkok")
        .select("city", "country")
    )
    .distinct()
    .orderBy("city")
)
