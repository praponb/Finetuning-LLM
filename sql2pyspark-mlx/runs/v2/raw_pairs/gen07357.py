from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "right")
    .groupBy(
        F.quarter(F.col("o.order_date")).alias("order_quarter"),
        F.col("o.channel"),
    )
    .agg(F.sort_array(F.collect_set(F.col("p.status"))).alias("status_list"))
    .orderBy(F.col("order_quarter").desc())
)
