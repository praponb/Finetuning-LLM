from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "status")
    .intersect(
        df_payments
        .filter(F.col("amount").between(810, 1310))
        .select("id", "status")
    )
)
