from pyspark.sql import functions as F

result = (
    df_orders
    .select("channel")
    .subtract(
        df_orders
        .filter(F.col("order_date").between("2025-01-09", "2025-04-01"))
        .select("channel")
    )
)
