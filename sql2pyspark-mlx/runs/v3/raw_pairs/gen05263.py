from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "left")
    .groupBy(F.col("o.status"))
    .agg(
        F.sum(F.when(F.col("p.method") == "card", F.col("p.amount")).otherwise(0)).alias("card_amount"),
    )
)
