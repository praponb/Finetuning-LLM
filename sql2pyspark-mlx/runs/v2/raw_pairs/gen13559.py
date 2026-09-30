from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("segment").isin(["Corporate", "SMB"]))
    .select("id", "signup_date", "segment")
)
