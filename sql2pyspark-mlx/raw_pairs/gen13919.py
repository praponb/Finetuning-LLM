from pyspark.sql import functions as F

src = (
    df_orders
    .filter(F.col("order_date").between("2023-11-14", "2025-08-01") & (F.col("amount") < 650))
)

order_month_summary = (
    src
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(
        F.count("*").alias("order_count"),
        F.countDistinct("amount").alias("unique_customers"),
    )
)

result = order_month_summary.select("order_month", "unique_customers")
