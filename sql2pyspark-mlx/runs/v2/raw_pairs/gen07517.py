from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("shipped_date").isin([1, 5, 10]))
    .select("id", "delivered_date", "cost", F.round("cost", 1).alias("cost_rounded"))
)
