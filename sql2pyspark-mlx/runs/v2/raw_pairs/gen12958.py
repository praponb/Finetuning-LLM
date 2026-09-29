from pyspark.sql import functions as F

avg_cost = df_shipments.agg(F.avg("cost")).first()[0]

result = (
    df_shipments
    .filter(F.col("cost") >= avg_cost)
    .select("id", "cost")
    .orderBy(F.col("cost").desc(), "id")
    .limit(50)
)
