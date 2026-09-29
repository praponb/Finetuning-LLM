from pyspark.sql import functions as F

result = (
    df_transactions
    .filter(F.col("amount") >= 1000)
    .select("id", "merchant")
    .unionByName(
        df_transactions
        .filter(F.col("amount") > 1300)
        .select("id", "merchant")
    )
    .distinct()
    .orderBy(F.col("id").desc())
)
