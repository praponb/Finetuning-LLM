from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") < 660)
    .select("channel", "order_date")
    .unionByName(
        df_orders
        .filter(F.col("amount") < 610)
        .select("channel", "order_date")
    )
    .orderBy("channel")
)
