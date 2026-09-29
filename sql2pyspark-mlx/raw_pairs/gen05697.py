from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("credit_limit").isNotNull())
    .select("signup_date")
    .union(df_shipments.select("delivered_date"))
    .distinct()
    .orderBy("signup_date")
)
