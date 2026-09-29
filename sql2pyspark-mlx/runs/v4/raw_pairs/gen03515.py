from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.year("order_date") == 2023)
    .select("channel", "status")
    .distinct()
    .orderBy("status")
)
