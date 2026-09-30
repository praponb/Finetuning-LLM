from pyspark.sql import functions as F

result = (
    df_accounts
    .filter(F.col("status").isin(["CLOSED", "FROZEN", "OPEN"]))
    .select("status", F.coalesce("status", F.lit("N/A")).alias("status_filled"))
)
