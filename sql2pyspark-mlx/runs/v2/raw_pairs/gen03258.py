from pyspark.sql import functions as F

max_shipped_date = df_shipments.filter(F.col("cost") < 62).agg(F.max("shipped_date")).first()[0]

result = (
    df_shipments
    .filter(F.col("shipped_date") >= max_shipped_date)
    .select("id", "shipped_date", "cost", "order_id")
    .orderBy(F.col("cost").desc(), F.col("order_id").desc())
)
