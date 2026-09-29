from pyspark.sql import functions as F

result = (
    df_shipments
    .select("id", "status", F.coalesce("cost", F.lit(0)).alias("cost_filled"))
)
