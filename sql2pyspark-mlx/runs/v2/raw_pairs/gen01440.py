from pyspark.sql import functions as F

max_cost = df_shipments.agg(F.max("cost")).first()[0]

result = (
    df_shipments
    .filter(F.col("cost") > max_cost)
    .select("id", "cost", "shipped_date", "carrier")
)
