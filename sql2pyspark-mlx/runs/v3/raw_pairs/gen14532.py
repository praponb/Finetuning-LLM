from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 1130)
    .select("method")
    .intersect(
        df_payments
        .filter(F.col("amount").between(870, 1460))
        .select("method")
    )
)
