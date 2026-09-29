from pyspark.sql import functions as F

result = (
    df_stores
    .select("opened_date")
    .intersect(df_orders.select("order_date"))
    .orderBy(F.col("opened_date").desc())
)
