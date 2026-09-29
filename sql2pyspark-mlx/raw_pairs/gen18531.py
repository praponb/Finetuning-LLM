from pyspark.sql import functions as F

result = (
    df_payments
    .select("method")
    .intersectAll(
        df_payments
        .filter(F.col("method").isin(["paypal", "cash", "card"]))
        .select("method")
    )
)
