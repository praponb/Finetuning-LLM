from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 750)
    .select("id", "method")
    .unionByName(
        df_payments
        .filter(F.col("amount").between(850, 1280))
        .select("id", "method")
    )
)
