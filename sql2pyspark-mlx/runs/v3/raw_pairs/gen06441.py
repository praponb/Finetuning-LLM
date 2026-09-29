from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date").between("2023-06-15", "2023-11-11"))
    .select("delivered_date")
    .intersect(df_orders.select("order_date"))
    .orderBy("delivered_date")
)
