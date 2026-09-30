from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("credit_limit") > 15800) & (F.col("email").isNotNull() | F.col("city").isin(["New York", "Bangkok", "Tokyo"])))
    .select("signup_date", "country")
    .orderBy("country")
)
