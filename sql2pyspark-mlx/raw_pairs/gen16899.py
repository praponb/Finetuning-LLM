from pyspark.sql import functions as F

result = (
    df_departments
    .select(F.col("location").alias("label"))
    .subtract(
        df_payments
        .filter(F.col("method").isin(["paypal", "bank_transfer"]))
        .select(F.col("method").alias("label"))
    )
    .orderBy("label")
)
