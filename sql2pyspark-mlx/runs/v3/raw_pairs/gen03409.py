from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower(F.trim("email")) == "emma@outlook.com")
    .select("id", "city")
    .subtract(
        df_customers
        .filter(F.col("country").isin(["JP", "UK", "DE"]))
        .select("id", "city")
    )
)
