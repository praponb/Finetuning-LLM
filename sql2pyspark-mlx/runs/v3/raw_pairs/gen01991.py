from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("status") != "COMPLETED")
    .select("order_date")
    .union(df_stores.select("opened_date"))
    .distinct()
    .orderBy(F.col("order_date").desc())
)
