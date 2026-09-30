from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(920, 930))
    .select(
        "id",
        "discount",
        F.when(F.col("amount") >= 1750, "High").when(F.col("amount") >= 420, "Medium").otherwise("Low").alias("amount_band"),
    )
)
