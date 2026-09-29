from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft") >= 7038)
    .select("opened_date")
    .union(
        df_orders
        .filter(F.col("shipping_fee").between(25, 35))
        .select("order_date")
    )
    .distinct()
    .orderBy(F.col("opened_date").desc())
)
