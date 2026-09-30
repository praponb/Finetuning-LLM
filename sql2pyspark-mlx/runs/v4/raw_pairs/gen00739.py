from pyspark.sql import functions as F

result = (
    df_transactions
    .select("merchant")
    .intersect(
        df_transactions
        .filter(F.col("is_flagged"))
        .select("merchant")
    )
)
