from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit") <= 15600)
    .select("id", "city", "name")
    .subtract(
        df_customers
        .filter(F.lower(F.trim("email")) == "bob.smith@yahoo.com")
        .select("id", "city", "name")
    )
)
