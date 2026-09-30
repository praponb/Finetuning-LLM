from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("cost") < 51)
    .select("cost", "shipped_date", "carrier", F.ceil("cost").alias("cost_whole"))
)
