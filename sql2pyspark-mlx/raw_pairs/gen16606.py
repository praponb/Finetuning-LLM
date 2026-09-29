from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isin(["web", "partner"]))
    .select("amount", "status", F.coalesce("amount", F.lit(0)).alias("amount_filled"))
)
