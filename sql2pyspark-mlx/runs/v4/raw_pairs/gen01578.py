from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .groupBy(
        F.month(F.col("o.order_date")).alias("order_month"),
        F.year(F.col("o.order_date")).alias("order_year"),
    )
    .agg(
        F.sum(F.col("s.cost")).alias("total_cost"),
        F.avg(F.col("s.cost")).alias("avg_cost"),
    )
    .orderBy(F.col("avg_cost").desc(), "order_month", "order_year")
    .limit(5)
)
