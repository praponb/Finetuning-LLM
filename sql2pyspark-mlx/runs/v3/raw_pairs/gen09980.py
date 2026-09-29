from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.month("delivered_date").isin([1, 7, 11]))
    .groupBy("carrier")
    .pivot("status", ["RETURNED", "DELIVERED", "IN_TRANSIT"])
    .agg(F.count("cost"))
    .orderBy("carrier")
)
