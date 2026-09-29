from pyspark.sql import functions as F

max_cost = df_shipments.filter(F.col("delivered_date") > "2025-05-03").agg(F.max("cost")).first()[0]

result = (
    df_shipments
    .filter(F.col("cost") >= max_cost)
    .select("id", "cost", "order_id", "shipped_date")
    .orderBy(F.col("id").desc())
)
