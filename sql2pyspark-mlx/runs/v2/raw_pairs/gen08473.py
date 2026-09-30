from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") >= "2023-07-01")
    .select("id", "shipped_date", F.round("cost", 2).alias("cost_rounded"))
    .orderBy(F.col("id").desc(), F.col("cost_rounded").desc())
    .limit(3)
)
