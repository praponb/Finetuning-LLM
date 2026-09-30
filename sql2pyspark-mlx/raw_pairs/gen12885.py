from pyspark.sql import functions as F

result = (
    df_shipments.alias("s")
    .join(df_orders.alias("o"), F.col("o.id") == F.col("s.order_id"), "inner")
    .filter(F.col("o.order_date").between("2023-08-03", "2024-04-01") & (F.col("o.amount") < 1080))
    .select(
        F.col("s.id"),
        F.col("s.cost"),
        F.col("o.order_date"),
        F.concat(F.col("s.status"), F.lit(" "), F.col("s.carrier")).alias("status_carrier"),
    )
)
