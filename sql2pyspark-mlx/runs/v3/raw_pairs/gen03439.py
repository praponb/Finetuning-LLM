from pyspark.sql import functions as F

result = (
    df_shipments
    .select("shipped_date")
    .intersect(df_customers.select("signup_date"))
    .orderBy(F.col("shipped_date").desc())
)
