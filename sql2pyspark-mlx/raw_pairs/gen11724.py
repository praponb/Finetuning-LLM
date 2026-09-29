from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("amount") < 2200)
        .select("id", "merchant")
    )
)
