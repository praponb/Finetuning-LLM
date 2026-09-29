from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("status").isNotNull())
    .select("status")
    .union(df_suppliers.filter(F.col("city").isNotNull()).select("city"))
    .distinct()
    .orderBy("status")
)
