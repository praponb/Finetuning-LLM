from pyspark.sql import functions as F

result = (
    df_payments.alias("p")
    .join(df_orders.alias("o"), F.col("p.order_id") == F.col("o.id"), "inner")
    .groupBy(
        F.quarter(F.col("o.order_date")).alias("order_quarter"),
        F.date_trunc("MONTH", F.col("o.order_date")).alias("order_month"),
    )
    .agg(
        F.countDistinct(F.col("p.amount")).alias("unique_orders"),
        F.round(((100 * F.sum(F.when(F.col("p.status") == "SUCCESS", 1).otherwise(0))) / F.count("*")), 1).alias("pct_success"),
    )
    .orderBy(F.col("unique_orders").desc())
)
