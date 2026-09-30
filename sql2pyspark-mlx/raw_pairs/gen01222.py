from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("cost").isNotNull())
    .select("cost")
    .subtract(df_payments.filter(F.col("amount").isNotNull()).select("amount"))
    .orderBy("cost")
)
