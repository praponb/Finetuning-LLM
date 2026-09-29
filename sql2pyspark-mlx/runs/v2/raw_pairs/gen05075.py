from pyspark.sql import functions as F

filtered_orders = (
    df_orders
    .filter((F.col("amount") > 1210) & F.col("order_date").between("2023-04-01", "2024-03-01"))
)

summary = (
    filtered_orders
    .groupBy(F.date_trunc("MONTH", "order_date").alias("order_month"))
    .agg(F.sort_array(F.collect_set("status")).alias("status_list"))
)

result = summary
