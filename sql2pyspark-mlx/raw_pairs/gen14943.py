from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "shipped_date", "carrier", F.ceil("cost").alias("cost_whole"))
    .orderBy(F.col("id").desc())
)
