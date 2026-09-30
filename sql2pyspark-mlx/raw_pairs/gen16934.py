from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("email").isNotNull())
    .select("email")
    .union(df_shipments.filter(F.col("carrier").isNotNull()).select("carrier"))
    .distinct()
)
