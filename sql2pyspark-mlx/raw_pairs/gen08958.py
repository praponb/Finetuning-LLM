from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("method").isNotNull())
    .select("method")
    .subtract(df_products.filter(F.col("brand").isNotNull()).select("brand"))
    .orderBy("method")
)
