from pyspark.sql import functions as F

result = (
    df_payments
    .select("id", "status")
    .intersect(
        df_payments
        .filter(F.col("status") == "REFUNDED")
        .select("id", "status")
    )
)
