from pyspark.sql import functions as F

result = (
    df_employees
    .select(F.lit("employee").alias("origin"), "email")
    .union(
        df_payments
        .filter(~F.col("method").isin(["card", "promptpay"]))
        .select(F.lit("payment").alias("origin"), "method")
    )
)
