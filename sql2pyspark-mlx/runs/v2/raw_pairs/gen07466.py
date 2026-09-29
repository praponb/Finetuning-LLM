from pyspark.sql import functions as F

max_cost = df_shipments.filter(F.col("status").isin(["RETURNED", "LOST"])).agg(F.max("cost")).first()[0]

result = (
    df_shipments
    .filter(F.col("cost") >= max_cost)
    .select("id", "cost", "status")
    .orderBy(F.col("cost").desc())
)
