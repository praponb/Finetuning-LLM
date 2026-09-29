from pyspark.sql import functions as F

result = (
    df_shipments
    .select("cost")
    .intersect(
        df_orders
        .filter(F.col("order_date").between("2024-04-01", "2024-10-01"))
        .select("amount")
    )
)
