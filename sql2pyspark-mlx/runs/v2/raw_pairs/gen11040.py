from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.year("order_date") == 2025) & F.col("store_id").isin([6, 7, 38]))
    .select(
        "id",
        "amount",
        "store_id",
        "order_date",
        F.when(F.col("amount") >= 1230, "High").when(F.col("amount") >= 590, "Medium").otherwise("Low").alias("amount_band"),
    )
    .orderBy(F.col("store_id").desc())
)
