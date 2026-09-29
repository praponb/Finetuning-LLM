from pyspark.sql import functions as F

result = (
    df_transactions
    .select("id")
    .exceptAll(
        df_transactions
        .filter(~F.col("merchant").isin(["Shell", "Grab"]))
        .select("id")
    )
)
