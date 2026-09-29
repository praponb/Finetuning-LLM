from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1570)
    .select("status")
    .intersect(df_customers.filter(F.col("segment") == "Corporate").select("country"))
    .orderBy("status")
)
