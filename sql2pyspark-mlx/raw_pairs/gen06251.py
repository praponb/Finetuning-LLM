from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 2, 5]) & (F.col("store_id").isin([3, 4, 5]) | F.col("store_id").isin([2, 3, 4, 5])))
    .select(
        "id",
        "store_id",
        F.date_trunc("WEEK", "sale_date").alias("sale_date_period"),
    )
)
