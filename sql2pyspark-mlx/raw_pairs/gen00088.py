from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") >= 1590)
    .groupBy("status")
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.countDistinct("amount").alias("unique_statuss"),
    )
    .filter(F.col("unique_statuss") >= 4)
    .drop("unique_statuss")
)
