from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "Flash") & (F.year("shipped_date") == 2025))
    .select("delivered_date", "cost", F.coalesce("cost", F.lit(0)).alias("cost_filled"))
)
