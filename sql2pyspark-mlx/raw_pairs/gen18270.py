from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id", "merchant")
    .subtract(
        df_transactions
        .filter(F.col("merchant").isin(["Starbucks", "Lazada"]))
        .select("id", "merchant")
    )
)
