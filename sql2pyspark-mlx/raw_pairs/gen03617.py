from pyspark.sql import functions as F

result = (
    df_payments
    .select("status")
    .intersect(
        df_payments
        .filter(F.col("amount") < 1410)
        .select("status")
    )
)
