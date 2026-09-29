from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["Corporate", "SMB", "Consumer"]))
    .select("signup_date", "city")
    .unionByName(df_customers.filter(F.col("is_active")).select("signup_date", "city"))
    .distinct()
)
