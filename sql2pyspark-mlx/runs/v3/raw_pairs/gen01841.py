from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") >= 1040)
    .select("status")
    .intersect(
        df_shipments
        .filter(F.col("delivered_date") >= "2024-11-01")
        .select("status")
    )
    .orderBy(F.col("status").desc())
)
