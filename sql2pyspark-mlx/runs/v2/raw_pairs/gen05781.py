from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("segment").isin(["Corporate", "SMB"]))
    .select("signup_date")
    .subtract(df_customers.filter(~F.col("is_active")).select("signup_date"))
    .orderBy("signup_date")
)
