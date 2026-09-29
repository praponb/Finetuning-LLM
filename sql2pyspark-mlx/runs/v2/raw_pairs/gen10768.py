from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("city").isin(["Bangkok", "Tokyo"]))
    .select("id", "country", "age")
)
