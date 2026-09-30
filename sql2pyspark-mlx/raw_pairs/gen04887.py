from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.month("order_date").isin([1, 3, 8]))
    .select("status", "channel")
    .distinct()
)
