from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("amount") > 920)
    .select(F.col("amount").alias("value"))
    .subtract(
        df_store_sales
        .filter(F.month("sale_date").isin([3, 6, 7]))
        .select(F.col("revenue").alias("value"))
    )
)
