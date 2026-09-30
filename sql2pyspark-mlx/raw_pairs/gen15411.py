from pyspark.sql import functions as F

dim_totals = (
    df_orders
    .alias("a")
    .join(df_customers.alias("c"), F.col("a.customer_id") == F.col("c.id"), "inner")
    .groupBy(F.col("c.segment").alias("segment"), F.col("a.status").alias("status"))
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("a.amount")), 2).alias("total_amount"),
    )
)

result = (
    dim_totals
    .select("segment", "status", "total_amount")
    .orderBy(F.col("status").desc())
)
