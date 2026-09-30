from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("city") == "New York")
    .select("email")
    .unionByName(
        df_customers
        .filter(F.col("city").isin(["Tokyo", "Chiang Mai", "Singapore"]))
        .select("email")
    )
)
