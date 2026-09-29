from pyspark.sql import functions as F

result = (
    df_customers
    .filter((F.col("country") == "TH") | (F.upper(F.trim("email")) == "EMMA@OUTLOOK.COM"))
    .select("id", "age")
    .orderBy(F.col("id").desc())
    .limit(5)
)
